# dataset-tools

[![Clojars Project](https://img.shields.io/clojars/v/dataset-tools.svg)](https://clojars.org/dataset-tools)

API documentation is [here](https://emiruz.github.io/dataset-tools/index.html).

An easy to use library for working with [core.matrix.dataset](https://mikera.github.io/core.matrix/doc/clojure.core.matrix.dataset.html)
datasets in Clojure. Library includes the following functions:

* select (column selection)
* order (multi-field sorting)
* where (filtering)
* join (inner and left join datasets on arbitrary criteria)
* aggregate (group by aggregates)
* cross-tab (pivot-tables)
* order-columns (column ordering)
* capply, rapply (column and row apply)
* to-dataset (list of maps to dataset)
* from-dataset (dataset to list of maps).
* from-csv (dataset from csv file)
* reduce-dimensions (dimension reduction of a dataset).
* various aux. functions.

Please report issues and contribute if you can.

## Getting Started

1. Add the library to your project file:

[![Clojars Project](https://img.shields.io/clojars/v/dataset-tools.svg)](https://clojars.org/dataset-tools)

2. Either *use* or *require* the library in your code:

```clojure
(require '[dataset-tools.core :as dt])
```

## Common Tasks

Every function other than *from-dataset* returns a dataset. The parameters of the functions
are designed such that they are easy to thread together to elegantly compose complex
processing tasks. Here is some example of common tasks.


```clojure
(def test-data
  [{:a 1 :b 4 :c "X" :d "A"}
   {:a 41 :b 33 :c "Y" :d "A"}
   {:a 12 :b 19 :c "X" :d "B"}])

(def test-data2
  [{:a 1 :e 9 :x "X"}
   {:a 1 :e 9 :x "X2"}
   {:a 41 :e 99 :x "A"}
   {:a 13 :e 999 :x "X"}])
```

### Convert a list of maps to a dataset

```clojure
(->> test-data
     (dt/to-dataset [:a :b :c :d]))
```

### Convert a dataset to a lazy sequence of maps

```clojure
(def test-dataset (dt/to-dataset [:a :b :c :d] test-data))

(->> test-dataset
     dt/from-dataset)
```

### Get a column vector

```clojure
(dt/select :a test-dataset)
```

### Filter the dataset to show rows where *c* = "Y", order the result by *a*,
then only show columns *a* and *b*

```clojure
(->> test-dataset
     (dt/where (comp #(= % "Y") :c))
     (dt/order :a)
     (dt/select [:a :b]))
```

### Produce sum(a) and mean(b), grouped by columns *c* and *d*

```clojure
(->> test-dataset
     (dt/aggregate [:c :d]
     		   {:sum (fn[v](reduce + 0 (map :c v)))
		    :mean (fn[v](/ (reduce + 0 (map :c v))) (count v))}))
```

### Inner join two datasets on column *a*

```clojure
(def test-dataset2 (dt/to-dataset [:a :e :x] test-data))

(dt/join :a
	 :a
	 test-dataset
	 test-dataset2)
```

### Produce a pivot of *c* versus *d* with sum(b) at the intersections

```clojure
(dt/cross-tab :c :d
	      #(if (nil? %) nil (reduce + 0 (map :b %)))
	      test-dataset)
```

## License

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
