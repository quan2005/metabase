(ns metabase.query-processor-test.middleware.fetch-source-query-test
  (:require [expectations :refer [expect]]
            [metabase.models
             [card :refer [Card]]
             [database :as database]]
            [metabase.query-processor.middleware.fetch-source-query :as fetch-source-query]
            [metabase.test.data :as data]
            [metabase.util :as u]
            [toucan.util.test :as tt]))

(def ^:private ^{:arglists '([query])} fetch-source-query (fetch-source-query/fetch-source-query identity))

;; make sure that the `fetch-source-query` middleware correctly resolves MBQL queries
(defn- x []
  {:database (data/id)
   :type     :query
   :query    {:aggregation  [:count]
              :breakout     [[:field-literal :price :type/Integer]]
              :source-query {:source-table (data/id :venues)}}}
  (tt/with-temp Card [card {:dataset_query {:database (data/id)
                                            :type     :query
                                            :query    {:source-table (data/id :venues)}}}]
    (fetch-source-query {:database database/virtual-id
                         :type     :query
                         :query    {:source-table (str "card__" (u/get-id card))
                                    :aggregation  [:count]
                                    :breakout     [[:field-literal :price :type/Integer]]}})))

;; make sure that the `fetch-source-query` middleware correctly resolves native queries
(expect
  {:database (data/id)
   :type     :query
   :query    {:aggregation  [:count]
              :breakout     [[:field-literal :price :type/Integer]]
              :source-query {:native (format "SELECT * FROM %s" (data/format-name "venues"))}}}
  (tt/with-temp Card [card {:dataset_query {:database (data/id)
                                            :type     :native
                                            :native   {:query (format "SELECT * FROM %s" (data/format-name "venues"))}}}]
    (fetch-source-query {:database database/virtual-id
                         :type     :query
                         :query    {:source-table (str "card__" (u/get-id card))
                                    :aggregation  [:count]
                                    :breakout     [[:field-literal :price :type/Integer]]}})))
