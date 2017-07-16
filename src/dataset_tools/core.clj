(ns dataset-tools.core
  (:require [clojure.string :as str]
            [clojure.core.matrix.dataset :as md]
            [clojure.core.matrix :as m]
            [clojure.set :as cset]
            [clojure.core.reducers :as red]))

(defn from-dataset[ds]
  "Returns a lazy sequence of maps from a dataset ds."
  (let [cols (md/column-names ds)]
    (map #(zipmap cols %) ds)))

(defn to-dataset[ks coll]
  "Converts a map sequence from coll to a dataset using only
   the keys specified in ks."
  (md/dataset
     ks
   (m/matrix
    (into [] (map
              (comp
               #(into[] (vals %))
               #(select-keys % ks))
              coll)))))

(defn select[cols ds]
  "Returns the dataset ds only containing columns specified in cols, 
   or returns a column vector if a single column is specified in cols."
  (let [cond (or (seq? cols) (vector? cols))
        c (if cond cols (conj [] cols))
        n (md/select-columns ds c)]
    (if cond n (m/get-column n 0))))

(defn where[pred ds]
  "Returns the dataset ds filtered on predicate pred. Pred is expected
   to be a unitary function which expects a map representation of each
   row as an input and returns true/false."
  (->> ds
       from-dataset
       (clojure.core/filter pred)
       (to-dataset (md/column-names ds))))

(defn order[cols ds]
  "Returns the dataset ds sorted in ascending order on the columns
   specified in cols."
  (->> ds
       from-dataset
       (sort-by (apply juxt (flatten [cols])))
       (to-dataset (md/column-names ds))))

(defn aggregate[cols f ds]
  "Returns dataset ds grouped by cols (list/vector), by
   function (or map of functions) f, a grouped aggregate.
   f can both be a function, or a map of functions, in which
   case the maps column names will be used as fields. Note that
   the function f is uniary and should expect a vector as input, and
   it should return a scalar as output."
  (let [c (flatten [cols])
        k (if (map? f) (keys f) '(:group-value))
        v (if (map? f) (vals f) [f])
        g (->> ds
               from-dataset
               (group-by #(select-keys % c))
               (into {}))]
    (->> (map #(zipmap k ((apply juxt v) %)) (vals g))
         (interleave (keys g))
         (partition 2)
         (map #(apply merge %))
         (to-dataset (concat c k)))))

(defn join[fx fy dsx dsy & {:keys [left] :or {:left false}}]
  "Join two datasets (dsx, dsy) using the key functions fx and fy.
   If :left is set to true then the join will be a left join, otherwise
   the join will be an inner join."
  (let [m (->> dsy
               from-dataset
               (group-by fy))
        xcols (md/column-names dsx)
        ycols (md/column-names dsy)
        ucols (distinct (concat xcols ycols))
        dcols (cset/difference (into #{} ycols)
                               (cset/intersection
                                (into #{} xcols)
                                (into #{} ycols)))]
    (->> dsx
         from-dataset
         (map (fn[x]
                (let [g (map #(merge x (select-keys % dcols)) (get m (fx x)))]
                  (if (and left (empty? g))
                    (reduce #(update %1 %2 (constantly nil)) x dcols) g))))
         flatten
         (to-dataset ucols))))

(defn cross-tab[cols col f ds]
  "Returns a cross-tabulation of dataset ds with the columns in cols
   forming the rows. The unique values of the column in col are used to
   form the columns. The intersection of rows and columns is calculated by
   passing the vector of values to f which is expected to return a scalar."
  (->> ds
       from-dataset
       (group-by #(select-keys % (conj (flatten [cols]) col)))
  ))
