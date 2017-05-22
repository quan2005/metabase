(ns metabase.query-processor.middleware.add-dimension-projections
  "Middleware for adding `:row_count` and `:status` info to QP results."
  (:require (metabase.query-processor [interface :as i]
                                      [util :as qputil])
            [metabase.models.field :refer [with-dimensions with-values]])
  (:import [metabase.query_processor.interface RemapExpression]))

(defn create-expression-col [alias]
  {:description nil,
   :id nil,
   :table_id nil,
   :expression-name alias,
   :source :fields,
   :name alias,
   :display_name alias,
   :target nil,
   :extra_info {}})

(defn row-map-fn [dim-seq]
  (fn [row]
    (concat row (map (fn [{:keys [col-index xform-map]}]
                       (xform-map (nth row col-index)))
                     dim-seq))))

(defn add-inline-remaps
  [qp]
  (fn [query]
    (let [results (qp query)
          indexed-dims (keep-indexed (fn [idx col]
                                       (when (seq (:dimensions col))
                                         {:col-index idx
                                          :name (get-in col [:dimensions :name])
                                          :xform-map (zipmap (get-in col [:values :values])
                                                             (get-in col [:values :human_readable_values]))
                                          :new-column (create-expression-col (get-in col [:dimensions :name]))}))
                                     (with-values (with-dimensions (filter :id (get-in results [:data :cols])))))
          remap-fn (row-map-fn indexed-dims)]

      (-> results
          (update-in [:data :columns] into (map :name indexed-dims))
          (update-in [:data :cols] into (map :new-column indexed-dims))
          (update-in [:data :rows] #(map remap-fn %))))))
