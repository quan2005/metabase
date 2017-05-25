(ns metabase.query-processor.middleware.source-table
  (:require [metabase.models.table :refer [Table]]
            [metabase.query-processor.util :as qputil]
            [toucan.db :as db]))

(defn resolve-source-table
  "Middleware that will take the source-table (an integer) and hydrate
  that source table from the the database and attach it as
  `:source-table`"
  [qp]
  (fn [{{source-table-id :source-table} :query :as expanded-query-dict}]
    (if-not (qputil/mbql-query? expanded-query-dict)
      (qp expanded-query-dict)
      (let [source-table (or (db/select-one [Table :schema :name :id], :id source-table-id)
                             (throw (Exception. (format "Query expansion failed: could not find source table %d." source-table-id))))]
        (-> expanded-query-dict
            (assoc-in [:query :source-table] source-table)
            qp)))))
