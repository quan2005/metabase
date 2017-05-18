(ns metabase.query-processor-test.nested-queries-test
  "Tests for handling queries with nested expressions."
  (:require [expectations :refer :all]
            [metabase.query-processor :as qp]
            [metabase.query-processor.expand :as ql]
            [metabase.query-processor-test :refer :all]
            [metabase.test.data :as data]
            [metabase.test.data.datasets :as datasets]
            #_[metabase.util :as u]))

;; NOCOMMIT
(defn- x []
  #_(require 'metabase.query-processor 'metabase.query-processor.expand :reload)
  (datasets/with-engine :postgres
    (qp/process-query
      {:database (data/id)
       :type     :query
       :query    {:source-query {:source-table (data/id :venues)
                                 :limit        10}
                  :limit        5}})))
;; -> SELECT * FROM (SELECT * FROM "public"."venues" LIMIT 10) "source" LIMIT 5

(defn- y []
  #_(require 'metabase.query-processor 'metabase.query-processor.expand :reload)
  (datasets/with-engine :postgres
    (qp/process-query
      {:database (data/id)
       :type     :query
       :query    {:source-query {:native "SELECT * FROM venues LIMIT 10"}
                  :limit        5}})))
;; -> SELECT * FROM (SELECT * FROM venues LIMIT 10) "source" LIMIT 5

(defn- z []
  #_(require 'metabase.query-processor 'metabase.query-processor.expand :reload)
  (datasets/with-engine :postgres
    (qp/process-query
      {:database (data/id)
       :type     :query
       :query    {:source-query {:native "SELECT * FROM venues"}
                  :breakout     [[:field-literal :price :type/Integer]]
                  :limit        5}})))
;; -> SELECT "price" AS "price" FROM (SELECT * FROM venues) "source" GROUP BY "price" ORDER BY "price" ASC LIMIT 5

;; ...should be the same as:
(defn- z2 []
  (datasets/with-engine :postgres
    (qp/process-query
      {:database (data/id)
       :type     :query
       :query    {:source-table (data/id :venues)
                  :breakout     [(data/id :venues :price)]
                  :limit        5}})))
;; -> SELECT "public"."venues"."price" AS "price" FROM "public"."venues" GROUP BY "public"."venues"."price" ORDER BY "public"."venues"."price" ASC LIMIT 5

(defn- a []
  #_(require 'metabase.query-processor 'metabase.query-processor.expand :reload)
  (datasets/with-engine :postgres
    (qp/process-query
      {:database (data/id)
       :type     :query
       :query    {:source-query {:source-table (data/id :venues)
                                 :limit        10}
                  :breakout     [[:field-literal :price :type/Integer]]
                  :limit        5}})))

;; -> SELECT "price" AS "price" FROM (SELECT * FROM "public"."venues" LIMIT 10) "source" GROUP BY "price" ORDER BY "price" ASC LIMIT 5

(expect
  [100]
  (x))

;; TODO - type information not coming back correctly !
;; TODO - need to get query permissions from the nested query as well as the outer query
;; TODO - If a nested query is a native query we need to check for *native* query permissions as well
