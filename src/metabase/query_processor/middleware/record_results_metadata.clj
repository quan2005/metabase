(ns metabase.query-processor.middleware.record-results-metadata
  "Middleware that stores metadata about results column types after running a query for a Card."
  (:require [metabase.util :as u]
            [toucan.db :as db]))

(defn- results->column-metadata
  "Return the desired storage format for the column metadata coming back from RESULTS, or `nil` if no columns were returned."
  [results]
  (seq (for [col (:cols results)]
         (u/select-non-nil-keys col [:name :display_name :description :base_type :special_type :unit]))))

(defn- record-metadata! [card-id results]
  (when-let [metadata (results->column-metadata results)]
    ;; TODO - it's definitiely inefficient to do this every single time a Card is ran. We should do something to mark the
    ;; card as "dirty" when its query changes (perhaps by clearing the value in `result_metadata` when query is modified)
    ;; and then only record new values when needed. Ideally we could do this without requiring an additional DB call to
    ;; check wheter the value is `NULL` which means this middleware would normally avoid the extra DB call altogether.
    ;; We should look into this more before merging, but for the proof-of-concept stage we're at right now the current
    ;; behavoir at least works correctly
    (db/update! 'Card card-id
      :result_metadata metadata)))

(defn record-results-metadata!
  "Middleware that records metadata about the columns returned when running the query if it is associated with a Card."
  [qp]
  (fn [{{:keys [card-id]} :info, :as query}]
    (u/prog1 (qp query)
      ;; TODO - do we need to check that the query returned successfully here, or will we not get here?
      (when card-id
        (record-metadata! card-id <>)))))
