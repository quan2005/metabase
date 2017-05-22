(ns metabase.models.dimensions
  (:require [toucan.models :as models]))

(def dimention-types
  #{:internal
    :external})

(models/defmodel Dimensions :metabase_dimensions)
