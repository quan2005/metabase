(ns metabase.query-processor.util
  "Utility functions used by the global query processor and middleware functions."
  (:require [buddy.core
             [codecs :as codecs]
             [hash :as hash]]
            [cheshire.core :as json]
            [clojure
             [string :as str]
             [walk :as walk]]
            [metabase.util.schema :as su]
            [schema.core :as s]
            [medley.core :as m]))

(defn mbql-query?
  "Is the given query an MBQL query?"
  [query]
  (= :query (keyword (:type query))))

(defn datetime-field?
  "Is FIELD a `DateTime` field?"
  [{:keys [base-type special-type]}]
  (or (isa? base-type :type/DateTime)
      (isa? special-type :type/DateTime)))

;; TODO - Not sure whether I like moving this here or whether it should go back to `metabase.query-processor.expand`. It does seem
;; like it has more to do with query expansion than anything else
(s/defn ^:always-validate normalize-token :- s/Keyword
  "Convert a string or keyword in various cases (`lisp-case`, `snake_case`, or `SCREAMING_SNAKE_CASE`) to a lisp-cased keyword."
  [token :- su/KeywordOrString]
  (-> (name token)
      str/lower-case
      (str/replace #"_" "-")
      keyword))

(defn normalize-keys
  "Normalize the keys in M into the usual MBQL lisp-case keyword format. Since MBQL itself accepts keys
   regardless of whether they're a string/keyword, lowercase/uppercase, or lisp-case/snake_case, this function
   facilitates various map operations on a raw MBQL query.

     (normalize-keys {\"NUM_TOUCANS\" 2, :total_birds 3}) ; -> {:num-toucans 2, :total-birds 3}

   This function recursively normalizes maps within the top-level map."
  [m]
  (walk/postwalk (fn [form]
                   (cond
                     (not (map? form)) form
                     (record? form)    form                                   ; record types should already be in lisp-case so nothing to do here
                     :else             (into {} (for [[k v] form]             ; all other maps should be normalized
                                                  {(normalize-token k) v}))))
                 m))

(defn query-without-aggregations-or-limits?
  "Is the given query an MBQL query without a `:limit`, `:aggregation`, or `:page` clause?"
  [{{aggregations :aggregation, :keys [limit page]} :query}]
  (and (not limit)
       (not page)
       (or (empty? aggregations)
           (= (:aggregation-type (first aggregations)) :rows))))

(defn query->remark
  "Genarate an approparite REMARK to be prepended to a query to give DBAs additional information about the query being executed.
   See documentation for `mbql->native` and [issue #2386](https://github.com/metabase/metabase/issues/2386) for more information."
  ^String [{{:keys [executed-by query-hash query-type], :as info} :info}]
  (str "Metabase" (when info
                    (assert (instance? (Class/forName "[B") query-hash))
                    (format ":: userID: %s queryType: %s queryHash: %s" executed-by query-type (codecs/bytes->hex query-hash)))))


;;; ------------------------------------------------------------ Hashing ------------------------------------------------------------

(defn- select-keys-for-hashing
  "Return QUERY with only the keys relevant to hashing kept.
   (This is done so irrelevant info or options that don't affect query results doesn't result in the same query producing different hashes.)"
  [query]
  {:pre [(map? query)]}
  (let [{:keys [constraints parameters], :as query} (select-keys query [:database :type :query :native :parameters :constraints])]
    (cond-> query
      (empty? constraints) (dissoc :constraints)
      (empty? parameters)  (dissoc :parameters))))

(defn query-hash
  "Return a 256-bit SHA3 hash of QUERY as a key for the cache. (This is returned as a byte array.)"
  [query]
  (hash/sha3-256 (json/generate-string (select-keys-for-hashing query))))
