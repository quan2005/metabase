(ns metabase.query-processor.middleware.add-dimension-projections-test
  "Tests for the Query Processor cache."
  (:require [expectations :refer :all]
            [metabase.query-processor.middleware.add-dimension-projections :refer :all]
            [metabase.query-processor.interface :as i]
            [metabase.test.util :as tu]
            [toucan.db :as db]
            [metabase.query-processor.expand :as ql]))

#_(ql/remap-expression (i/map->Field {:description nil,
                                         :table_id 4,
                                         :schema_name "PUBLIC",
                                         :special_type :type/FK,
                                         :name "CATEGORY_ID",
                                         :source :fields,
                                         :extra_info {:target_table_id 1},
                                         :fk_field_id nil,
                                         :id 11,
                                         :values nil
                                         :visibility_type :normal,
                                         :target {:id 1, :name "ID", :display_name "ID",
                                                  :table_id 1, :description nil,
                                                  :base_type :type/BigInteger, :special_type :type/PK,
                                                  :visibility_type :normal},
                                         :display_name "Category ID",
                                         :base_type :type/Integer}))

#_{:operator :*,
   :args
   [{:description "The rating (on a scale of 1-5) the user left.",
     :base-type :type/Integer,
     :table-id 4,
     :special-type :type/Category,
     :field-name "RATING",
     :field-display-name "Rating",
     :values {:id 1, :human_readable_values ["Horrible" "Bad" "Meh" "Pretty Good" "Excellent"], :values [1 2 3 4 5], :field_id 33},
     :visibility-type :normal,
     :field-id 33,
     :table-name "REVIEWS",
     :schema-name "PUBLIC"}
    5.0]}

(def query-with-expression
  {:query
   {:expressions
    {:Foo
     (ql/remap-expression (i/map->Field {:description nil,
                                         :table_id 4,
                                         :schema_name "PUBLIC",
                                         :special_type :type/FK,
                                         :name "CATEGORY_ID",
                                         :source :fields,
                                         :extra_info {:target_table_id 1},
                                         :fk_field_id nil,
                                         :id 11,
                                         :values {:id 1, :human_readable_values ["Foo" "Bar" "Baz" "Qux"],
                                                  :values [4 11 29 20], :field_id 33}
                                         :visibility_type :normal,
                                         :target nil,
                                         :display_name "Category ID",
                                         :base_type :type/Integer})
                          nil)}}})

(def example-resultset
  {:rows
   [[1 "Red Medicine" 4 3]
    [2 "Stout Burgers & Beers" 11 2]
    [3 "The Apple Pan" 11 2]
    [4 "Wurstküche" 29 2]
    [5 "Brite Spot Family Restaurant" 20 2]],
   :columns ["ID" "NAME" "CATEGORY_ID" "PRICE"],
   :cols
   [{:description nil,
     :table_id 4,
     :schema_name "PUBLIC",
     :special_type :type/PK,
     :name "ID",
     :source :fields,
     :extra_info {},
     :fk_field_id nil,
     :id 12,
     :values nil,
     :visibility_type :normal,
     :target nil,
     :display_name "ID",
     :base_type :type/BigInteger}
    {:description nil,
     :table_id 4,
     :schema_name "PUBLIC",
     :special_type :type/Name,
     :name "NAME",
     :source :fields,
     :extra_info {},
     :fk_field_id nil,
     :id 15,
     :values nil,
     :visibility_type :normal,
     :target nil,
     :display_name "Name",
     :base_type :type/Text}
    {:description nil,
     :table_id 4,
     :schema_name "PUBLIC",
     :special_type :type/FK,
     :name "CATEGORY_ID",
     :source :fields,
     :extra_info {:target_table_id 1},
     :fk_field_id nil,
     :id 11,
     :values nil,
     :visibility_type :normal,
     :target nil,
     :display_name "Category ID",
     :base_type :type/Integer}
    {:description nil,
     :table_id 4,
     :schema_name "PUBLIC",
     :special_type :type/Category,
     :name "PRICE",
     :source :fields,
     :extra_info {},
     :fk_field_id nil,
     :id 16,
     :values nil,
     :visibility_type :normal,
     :target nil,
     :display_name "Price",
     :base_type :type/Integer}]})

(expect
  (-> example-resultset
      (assoc :rows [[1 "Red Medicine" 4 3 "Foo"]
                    [2 "Stout Burgers & Beers" 11 2 "Bar"]
                    [3 "The Apple Pan" 11 2 "Bar"]
                    [4 "Wurstküche" 29 2 "Baz"]
                    [5 "Brite Spot Family Restaurant" 20 2 "Qux"]])
      (update :columns conj "Foo")
      (update :cols conj   {:description nil,
                            :id nil,
                            :table_id nil,
                            :expression-name "Foo",
                            :source :fields,
                            :name "Foo",
                            :display_name "Foo",
                            :target nil,
                            :extra_info {}}))
  ((add-inline-remaps (constantly example-resultset)) query-with-expression))
