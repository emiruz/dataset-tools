(ns dataset-tools.core
  "A versatile set of functions for working with core.matrix.dataset
   datasets. Please see the examples for usage."
  (:require [clojure.string :as str]
            [clojure.core.matrix.dataset :as md]
            [clojure.core.matrix :as m]
            [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clatrix.core :as cla]))

(defn- diff [v l] (filter (fn[x] (nil? (some #(= x %) l))) v))

(defn column-names
  "Returns the column names of the dataset ds. If except is specified
   it will not include columns listed therein."
  [ds & {:keys[except] :or {except nil}}]
  (diff (md/column-names ds) except))

(defn select-vals
  "Select the values from the map m in the order specified in ks."
  [m ks]
  (reduce #(conj %1 (get m %2)) [] ks))

(defn from-dataset
  "Returns a lazy sequence of maps from a dataset ds."
  [ds]
  (let [cols (md/column-names ds)]
    (map #(zipmap cols %) ds)))

(defn to-dataset
  "Converts a map sequence from coll to a dataset using only
   the keys specified in ks."
  [ks coll]
  (md/dataset
     ks
     (->> coll
          (map #(select-keys % ks))
          (map #(select-vals % ks))
          (into [])
          m/matrix
          )))

(defn select
  "Returns the dataset ds only containing columns specified in cols, 
   or returns a column vector if a single column is specified in cols."
  [cols ds]
  (let [o (->> (from-dataset ds) (to-dataset (flatten [cols])))]
    (if (or (seq? cols) (vector? cols))
      o (m/get-column o 0))))

(defn where
  "Returns the dataset ds filtered on predicate pred. Pred is expected
   to be a unitary function which expects a map representation of each
   row as an input and returns true/false."
  [pred ds]
  (->> ds
       from-dataset
       (clojure.core/filter pred)
       (to-dataset (column-names ds))))

(defn order
  "Returns the dataset ds sorted in ascending order on the columns
   specified in cols. If :rev keyword (false by default) is set to true
   then the sort order is reversed."
  [cols ds & {:keys [rev] :or {rev false}}]
  (->> ds
       from-dataset
       (sort-by (apply juxt (flatten [cols])))
       ((if rev reverse identity))
       (to-dataset (column-names ds))))

(defn aggregate
  "Returns dataset ds grouped by cols (list/vector), by
   function (or map of functions) f, a grouped aggregate.
   f can both be a function, or a map of functions, in which
   case the maps column names will be used as fields. Note that
   the function f is uniary and should expect a vector as input, and
   it should return a scalar as output."
  [cols f ds]
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

(defn join
  "Join two datasets (dsx, dsy) using the key functions fx and fy.
   If :left is set to true then the join will be a left join, otherwise
   the join will be an inner join."
  [fx fy dsx dsy & {:keys [left] :or {:left false}}]
  (let [m (->> dsy
               from-dataset
               (group-by fy))
        xcols (md/column-names dsx)
        ycols (md/column-names dsy)
        ucols (distinct (concat xcols ycols))
        dcols (diff ycols xcols)]
    (->> dsx
         from-dataset
         (map (fn[x]
                (let [g (map #(merge x (select-keys % dcols)) (get m (fx x)))]
                  (if (and left (empty? g))
                    (reduce #(update %1 %2 (constantly nil)) x dcols) g))))
         flatten
         (to-dataset ucols))))

(defn cross-tab
  "Returns a cross-tabulation of dataset ds with the columns in cols
   forming the rows. The unique values of the column in col are used to
   form the columns. The intersection of rows and columns is calculated by
   passing the vector of values to f which is expected to return a scalar."
  [cols col f ds]
  (let [d (distinct (select col ds))
        xcols (flatten [cols])]
        (->> ds
             from-dataset
             (group-by #(select-keys % xcols))
             (map
              (fn[x]
                (let [g (into {} (group-by #(get % col) (peek x)))
                      m (first x)]
                  (reduce
                   (fn[y z] (update y z (constantly (f (get g z))))) m d))))
             (to-dataset (concat xcols d))
             )))

(defn add-column
  "Adds column vector v with name n to dataset ds."
  [n v ds]
  (md/add-column ds n v))

(defn rapply
  "For each row, applies the function f. A vector is passed to f,
   and a scalar return is expected."
  [cols f ds]
  (->> (select (flatten [cols]) ds)
       (mapv #(f (vec %)))))

(defn capply
  "For each column, applies the function f. A vector is passed to f,
   and a scalar return is expected."
  [cols f ds]
  (let [o (select cols ds)]
    (mapv #(f (m/get-column o %))
          (range 0 (m/column-count o)))))

(defn order-columns
  "Sorts the dataset ds columns cols according to function f.
   The columns in prefix and suffix are added to the beginning and end
   of the dataset respectively. The function f should expect a column
   vector and return a scalar. If rev is specified the order is reversed."
  [cols f ds & {:keys [rev prefix suffix] :or {rev false prefix [] suffix []}}]
  (let [m (zipmap (capply cols f ds) cols)
        s (comp (if rev reverse identity) sort)
        c (concat (flatten [prefix])
                  (select-vals m (s (keys m)))
                  (flatten [suffix]))]
    (select c ds)))

(defn remove-column
  "Removes the columns cols from the dataset ds."
  [cols ds]
  (select (column-names ds :except cols) ds))

(defn reduce-dimensions
  "Removes some of the columns specified in cols from the dataset ds
   such that the fraction thres of variance is retainted. thres should
   be between 0 and 1, the cols specified should only be numeric."
  [thres cols ds]
  (let [capply (fn [f m](mapv #(f (m/get-column m %))
                              (range 0 (m/column-count m))))
        ds0 (select cols ds)
        m (cla/svd (cla/matrix (m/matrix ds0)))
        l (m/matrix (:left m))
        r (m/transpose (m/matrix (:right m)))
        s (m/matrix (:values m))
        sum (reduce + s)
        ind (distinct (conj (keep-indexed #(if (<= %2 thres) %1)
                          (map #(/ % sum) (reductions + s))) 0))        
        est (m/mmul (m/select l :all ind)
                    (m/diagonal-matrix (take (count ind) s))
                    (m/select r ind :all))
        lsq (capply (comp (partial reduce +) (partial map #(* % %)))
                    (m/add ds0 (m/scale est -1)))
        lsum (reduce + lsq)
        lcum (next (reduce #(conj %1 [(+ (->> %1 last first) (first %2)) (peek %2)]) [[0 0]]
                           (sort-by first (keep-indexed (fn[x y][(/ y lsum) x]) lsq))))
        lind (distinct (conj (filter #(if (<= (first %) thres) true) lcum) (first lcum)))
        cols0 (column-names ds0)]
    (select (concat (diff (column-names ds) cols)
                    (map #(get cols0 %) (map peek lind))) ds)))

(defn from-csv
  "Create a new dataset from a proper csv file f. sep and quo are optional
   keys and are expected to be characters indicating the separator and
   quoting character respectively."
  [f & {:keys [sep quo] :or {sep \, quo \"}}]
  (with-open [r (io/reader f)]
    (let [rr (fn[](csv/read-csv r :separator sep :quote quo))
          h (first (rr))
          v (fn[x](try (Double/parseDouble (str x)) (catch Exception e (str x))))]
      (to-dataset h (map (comp (partial zipmap h) (partial map v)) (rr))))))

