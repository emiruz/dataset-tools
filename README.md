TODO
* Tolerate missing keys in to-dataset function?
* finish cross-tab function.
* write tests for cross-tab and aggregate.
* finish documentation examples.
* generate documentation using codox.
* fix the links.
* generate a clojar jar.

# dataset-tools

[![Clojars Project](https://img.shields.io/clojars/v/dataset-tools.svg)](https://clojars.org/dataset-tools)

An easy to use library for working with [core.matrix.dataset](https://mikera.github.io/core.matrix/doc/clojure.core.matrix.dataset.html)
datasets in Clojure. Library includes the following functions:

* select (column selection)
* order (multi-field sorting)
* where (filtering)
* join (inner and left join datasets on arbitrary criteria)
* aggregate (group by aggregates)
* cross-tab (pivot-tables)
* to-dataset (list of maps to dataset)
* from-dataset (dataset to list of maps).

The API documentation is [here](...). 

## Getting Started

1. Add the library to your project file:

[![Clojars Project](https://img.shields.io/clojars/v/dataset-tools.svg)](https://clojars.org/dataset-tools)

2. Either *use* or *require* the library in your code:

```clojure
(require '[dataset-tools :as dt])
```

## Examples

Here is some example data:

```clojure
(def test-data
  [{:a 1 :b 4 :c "X" :d "A"}
   {:a 41 :b 33 :c "Y" :d "A"}
   {:a 12 :b 19 :c "X" :d "B"}])
```

1. Convert a list of maps to a dataset.

```clojure
(->> test-data
     (dt/to-dataset [:a :b :c :d]))
```

2. Convert a dataset to a lazy sequence of maps.

```clojure
(def test-dataset (dt/to-dataset [:a :b :c :d] test-data))

(->> test-dataset
     dt/from-dataset)
```

3. Get a column vector.

```clojure
(dt/select :a test-dataset)
```

4. Filter the dataset to show rows where *c* = "Y", order the result by *a*,
then only show columns *a* and *b*.

```clojure
(->> test-dataset
     (dt/where (comp #(= % "Y") :c))
     (dt/order :a)
     (dt/select [:a :b]))
```

5. Produce sum(*a*) and mean(*b*), grouped by columns *c* and *d*.

```clojure
(->> test-dataset
     (dt/aggregate [:c :d] {:sum (fn[v](reduce + 0 (map :c v)))
                            :mean (fn[v](/ (reduce + 0 (map :c v))) (count v))}))
```

6. Inner join two datasets on column *a*.

```clojure

```

7. Produce a pivot of *c* versus *d* with sum(*b*) at the intersections.

```clojure

```

8. Convert a list of maps to a dataset, filter out some rows, select only
some columns, create a sum of *a* grouped by *c* and *d*, pivot *c* versus *d*
to create a cross tabulation using *a* as-is, then (because we can...), left
join it to the original dataset.

```clojure

```

## License

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
