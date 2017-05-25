(ns metabase.query-processor-test.expressions-test
  "Tests for expressions (calculated columns)."
  (:require [expectations :refer :all]
            [metabase
             [query-processor-test :refer :all]
             [util :as u]]
            [metabase.query-processor.interface :as qpi]
            [metabase.query-processor.middleware.expand :as ql]
            [metabase.test.data :as data]
            [metabase.test.data.datasets :as datasets]))

;; Test the expansion of the expressions clause
(expect
  {:expressions {:my-cool-new-field (qpi/map->ArithmeticExpression {:operator :*
                                                                    :args [{:field-id 10, :fk-field-id nil, :datetime-unit nil}
                                                                           20.0]})}} ; 20 should be converted to a FLOAT
  (ql/expressions {} {:my-cool-new-field (ql/* (ql/field-id 10) 20)}))


;; Do a basic query including an expression
(datasets/expect-with-engines (engines-that-support :expressions)
  [[1 "Red Medicine"                 4  10.0646 -165.374 3 5.0]
   [2 "Stout Burgers & Beers"        11 34.0996 -118.329 2 4.0]
   [3 "The Apple Pan"                11 34.0406 -118.428 2 4.0]
   [4 "Wurstküche"                   29 33.9997 -118.465 2 4.0]
   [5 "Brite Spot Family Restaurant" 20 34.0778 -118.261 2 4.0]]
  (format-rows-by [int str int (partial u/round-to-decimals 4) (partial u/round-to-decimals 4) int float]
    (rows (data/run-query venues
            (ql/expressions {:my-cool-new-field (ql/+ $price 2)})
            (ql/limit 5)
            (ql/order-by (ql/asc $id))))))

;; Make sure FLOATING POINT division is done
(datasets/expect-with-engines (engines-that-support :expressions)
  [[1 "Red Medicine"           4 10.0646 -165.374 3 1.5]     ; 3 / 2 SHOULD BE 1.5, NOT 1 (!)
   [2 "Stout Burgers & Beers" 11 34.0996 -118.329 2 1.0]
   [3 "The Apple Pan"         11 34.0406 -118.428 2 1.0]]
  (format-rows-by [int str int (partial u/round-to-decimals 4) (partial u/round-to-decimals 4) int float]
    (rows (data/run-query venues
            (ql/expressions {:my-cool-new-field (ql// $price 2)})
            (ql/limit 3)
            (ql/order-by (ql/asc $id))))))

;; Can we do NESTED EXPRESSIONS ?
(datasets/expect-with-engines (engines-that-support :expressions)
  [[1 "Red Medicine"           4 10.0646 -165.374 3 3.0]
   [2 "Stout Burgers & Beers" 11 34.0996 -118.329 2 2.0]
   [3 "The Apple Pan"         11 34.0406 -118.428 2 2.0]]
  (format-rows-by [int str int (partial u/round-to-decimals 4) (partial u/round-to-decimals 4) int float]
    (rows (data/run-query venues
            (ql/expressions {:wow (ql/- (ql/* $price 2) (ql/+ $price 0))})
            (ql/limit 3)
            (ql/order-by (ql/asc $id))))))

;; Can we have MULTIPLE EXPRESSIONS?
(datasets/expect-with-engines (engines-that-support :expressions)
  [[1 "Red Medicine"           4 10.0646 -165.374 3 2.0 4.0]
   [2 "Stout Burgers & Beers" 11 34.0996 -118.329 2 1.0 3.0]
   [3 "The Apple Pan"         11 34.0406 -118.428 2 1.0 3.0]]
  (format-rows-by [int str int (partial u/round-to-decimals 4) (partial u/round-to-decimals 4) int float float]
    (rows (data/run-query venues
            (ql/expressions {:x (ql/- $price 1)
                             :y (ql/+ $price 1)})
            (ql/limit 3)
            (ql/order-by (ql/asc $id))))))

;; Can we refer to expressions inside a FIELDS clause?
(datasets/expect-with-engines (engines-that-support :expressions)
  [[4] [4] [5]]
  (format-rows-by [int]
    (rows (data/run-query venues
            (ql/expressions {:x (ql/+ $price $id)})
            (ql/fields (ql/expression :x))
            (ql/limit 3)
            (ql/order-by (ql/asc $id))))))

;; Can we refer to expressions inside an ORDER BY clause?
(datasets/expect-with-engines (engines-that-support :expressions)
  [[100 "Mohawk Bend"         46 34.0777 -118.265 2 102.0]
   [99  "Golden Road Brewing" 10 34.1505 -118.274 2 101.0]
   [98  "Lucky Baldwin's Pub"  7 34.1454 -118.149 2 100.0]]
  (format-rows-by [int str int (partial u/round-to-decimals 4) (partial u/round-to-decimals 4) int float]
    (rows (data/run-query venues
            (ql/expressions {:x (ql/+ $price $id)})
            (ql/limit 3)
            (ql/order-by (ql/desc (ql/expression :x)))))))

;; Can we AGGREGATE + BREAKOUT by an EXPRESSION?
(datasets/expect-with-engines (engines-that-support :expressions)
  [[2 22] [4 59] [6 13] [8 6]]
  (format-rows-by [int int]
    (rows (data/run-query venues
            (ql/expressions {:x (ql/* $price 2.0)})
            (ql/aggregation (ql/count))
            (ql/breakout (ql/expression :x))))))

(datasets/expect-with-engines (engines-that-support :expressions)
  {:columns ["ID" "NAME" "CATEGORY_ID" "LATITUDE" "LONGITUDE" "PRICE" "name-with-quotes"]
   :rows [[1 "Red Medicine"                 4  10.0646 -165.374 3 "[Red Medicine]"]
          [2 "Stout Burgers & Beers"        11 34.0996 -118.329 2 "[Stout Burgers & Beers]"]
          [3 "The Apple Pan"                11 34.0406 -118.428 2 "[The Apple Pan]"]
          [4 "Wurstküche"                   29 33.9997 -118.465 2 "[Wurstküche]"]
          [5 "Brite Spot Family Restaurant" 20 34.0778 -118.261 2 "[Brite Spot Family Restaurant]"]]}
  (format-rows-by [int str int (partial u/round-to-decimals 4) (partial u/round-to-decimals 4) int str]
    (rows+column-names
      (data/run-query venues
        (ql/expressions {:name-with-quotes (ql/sql-expression "'[' || name || ']'")})
        (ql/limit 5)
        (ql/order-by (ql/asc $id))))))

(datasets/expect-with-engines (engines-that-support :expressions)
  {:columns ["ID" "NAME" "CATEGORY_ID" "LATITUDE" "LONGITUDE" "PRICE" "remapped-val"]
   :rows [[1 "Red Medicine"                 4  10.0646 -165.374 3 "Foo"]
          [2 "Stout Burgers & Beers"        11 34.0996 -118.329 2 "Bar"]
          [3 "The Apple Pan"                11 34.0406 -118.428 2 "Bar"]
          [4 "Wurstküche"                   29 33.9997 -118.465 2 "Baz"]
          [5 "Brite Spot Family Restaurant" 20 34.0778 -118.261 2 "Qux"]]}
  (format-rows-by [int str int (partial u/round-to-decimals 4) (partial u/round-to-decimals 4) int str]
    (rows+column-names
      (data/run-query venues
        (ql/expressions {:remapped-val (ql/remap-expression $category_id [{:id 4  :value "Foo"}
                                                                          {:id 11 :value "Bar"}
                                                                          {:id 29 :value "Baz"}
                                                                          {:id 20 :value "Qux"}])})
        (ql/limit 5)
        (ql/order-by (ql/asc $id))))))

(datasets/expect-with-engines (engines-that-support :expressions)
  {:columns ["ID" "NAME" "CATEGORY_ID" "LATITUDE" "LONGITUDE" "PRICE" "remapped-val"]
   :rows [[1 "Red Medicine"                 4  10.0646 -165.374 3 "here"]
          [2 "Stout Burgers & Beers"        11 34.0996 -118.329 2 "there"]
          [3 "The Apple Pan"                11 34.0406 -118.428 2 nil]
          [4 "Wurstküche"                   29 33.9997 -118.465 2 nil]
          [5 "Brite Spot Family Restaurant" 20 34.0778 -118.261 2 nil]]}
  (format-rows-by [int str int (partial u/round-to-decimals 4) (partial u/round-to-decimals 4) int str]
    (rows+column-names
      (data/run-query venues
        (ql/expressions {:remapped-val (ql/remap-expression $latitude [{:id 10.0646 :value "here"}
                                                                       {:id 34.0996 :value "there"}])})
        (ql/limit 5)
        (ql/order-by (ql/asc $id))))))

(datasets/expect-with-engines (engines-that-support :expressions)
  {:columns ["ID" "NAME" "CATEGORY_ID" "LATITUDE" "LONGITUDE" "PRICE" "remapped-val"]
   :rows [[1 "Red Medicine"                 4  10.0646 -165.374 3 "Blue Medicine"]
          [2 "Stout Burgers & Beers"        11 34.0996 -118.329 2 "Porter Burgers & Beers"]
          [3 "The Apple Pan"                11 34.0406 -118.428 2 nil]
          [4 "Wurstküche"                   29 33.9997 -118.465 2 nil]
          [5 "Brite Spot Family Restaurant" 20 34.0778 -118.261 2 nil]]}
  (format-rows-by [int str int (partial u/round-to-decimals 4) (partial u/round-to-decimals 4) int str]
    (rows+column-names
      (data/run-query venues
        (ql/expressions {:remapped-val (ql/remap-expression $name [{:id "Red Medicine"          :value "Blue Medicine"}
                                                                   {:id "Stout Burgers & Beers" :value "Porter Burgers & Beers"}])})
        (ql/limit 5)
        (ql/order-by (ql/asc $id))))))

(datasets/expect-with-engines (engines-that-support :expressions)
  {:columns ["ID" "DATE" "USER_ID" "VENUE_ID" "remap-1" "remap-2"]
   :rows [[1 "2014-04-07T00:00:00.000Z" 5 12 "The Misfit Restaurant + Bar" "Quentin Sören"]
          [2 "2014-09-18T00:00:00.000Z" 1 31 "Bludso's BBQ" "Plato Yeshua"]
          [3 "2014-09-15T00:00:00.000Z" 8 56 "Philippe the Original" "Szymon Theutrich"]
          [4 "2014-03-11T00:00:00.000Z" 5 4 "Wurstküche" "Quentin Sören"]
          [5 "2013-05-05T00:00:00.000Z" 3 49 "Hotel Biron" "Kaneonuskatew Eiran"]]}
  (format-rows-by [int str int int str str]
    (rows+column-names
      (data/run-query checkins
        (ql/expressions {:remap-1 (ql/fk-remap-expression $venue_id $venue_id->venues.id $venue_id->venues.name)
                         :remap-2 (ql/fk-remap-expression $user_id $user_id->users.id $user_id->users.name)})
        (ql/limit 5)
        (ql/order-by (ql/asc $id))))))
