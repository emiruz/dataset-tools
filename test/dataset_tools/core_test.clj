(ns dataset-tools.core-test
  (:require [clojure.test :refer :all]
            [dataset-tools.core :as d]))

(def test-data
  [{:a 1 :b 4 :c "X" :d "A"}
   {:a 41 :b 33 :c "Y" :d "A"}
   {:a 12 :b 19 :c "X" :d "B"}])

(def test-data2
  [{:a 1 :e 9 :x "X"}
   {:a 1 :e 9 :x "X2"}
   {:a 41 :e 99 :x "A"}
   {:a 13 :e 999 :x "X"}])

(deftest to-and-from-dataset-test
  (testing
      "To/from dataset tests"
   (is (->> (d/to-dataset [:a :b :c :d] test-data)
            d/from-dataset
            (= test-data)))))

(deftest select-test
  (testing
      "Multi column select test"
    (is (->> (d/to-dataset [:a :b :c] test-data)
             (d/select [:a :c])
             d/from-dataset
             (map keys)
             (reduce concat ())
             distinct
             (map #(when (= :b %) true))
             (some some?)
             nil?)))
  (testing
      "Column vector select test"
    (is (->> (d/to-dataset [:a :b :c] test-data)
             (d/select :a)
             (= (into [] (map :a test-data)))))))

(deftest where-test
  (testing
      "Basic filter test"
    (is (->> (d/to-dataset [:a :b :c :d] test-data)
             (d/where (comp #(= "Y" %) :c))
             d/from-dataset
             (= (filter #(= (:c %) "Y") test-data))
             ))))

(deftest sort-test
  (testing
      "Single column sort test"
    (is (->> (d/to-dataset [:a :b :c] test-data)
             (d/order [:c])
             (d/select :c)
             (= (sort (map :c test-data)))
             ))))

(deftest aggregate-test
  (let [r1 (fn[v](reduce + (map :a v)))]
    (testing "Single column, single aggregate test"
      (is (->> (d/to-dataset [:a :b :c] test-data)
               (d/aggregate :c r1)
               (d/from-dataset)
               (= '({:c "X", :group-value 13} {:c "Y", :group-value 41})))))
    (testing "Multi column, multi aggregate test"
      (is (->> (d/to-dataset [:a :b :c :d] test-data)
               (d/aggregate [:c :d] {:r1 r1 :r2 r1})
               (d/from-dataset)
               (= '({:c "X", :d "A", :r1 1, :r2 1}
                    {:c "Y", :d "A", :r1 41, :r2 41}
                    {:c "X", :d "B", :r1 12, :r2 12})))))))
