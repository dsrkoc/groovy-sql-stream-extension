groovy-sql-stream-extensions
============================

Extends `groovy.sql.Sql` with stream capabilities to let you process query results
by applying higher order functions on a stream. This combines the convenience of
processing a List returned by `Sql#rows()` with the efficiency of looping directly
over ResultSet using `Sql#eachRow()`. The library is thus well suited for non-trivial
processing of a large data set.

Getting started
---------------

For the extension module to be functional, Groovy 2.0 or greater is required.

Usage:

    @Grab('hr.helix:groovy-sql-stream-extension:0.4.3')

Building from source:

1. clone the project
2. run `./gradlew build`
3. put the jar on the classpath

Examples
--------

Here is an example of (quite meaningless) data set processing, written in three
flavours:

1. using `Sql#rows()`,
2. using `Sql#eachRow()`, and finally
3. using `Sql#withStream()`

Example 1: convenient but not efficient

```groovy
def sql = groovy.sql.Sql(...)

sql.rows('SELECT * FROM foo_table')
    .collectMany { row ->
        [row.col_a + row.col_b, row.col_c * 2]
    }.collect {
        it < 10 ? it * 10 : it + 10
    }.findAll {
        it % 2 == 0
    }
```

Example 2: efficient but hardly satisfying

```groovy
def sql = ...

def result = []
sql.eachRow('...') { row ->
    def x = row.col_a + row.col_b,
        y = row.col_c * 2
    x = x < 10 ? x * 10 : x + 10
    y = y < 10 ? y * 10 : y + 10

    if (x % 2 == 0)
        result << x
    if (y % 2 == 0)
        result << y
}
```

Example 3: both efficient and convenient

```groovy
def sql = ...
sql.withStream('...') { stream ->
    stream.collectMany { row ->
        [row.col_a + row.col_b, row.col_c * 2]
    }.collect {
        it < 10 ? it * 10 : it + 10
    }.findAll {
        it % 2 == 0
    }.toList()
}
```

*Note on efficiency*: using `withStream()` may still be slightly less efficient than `eachRow()`,
depending on the respective implementation. However, it will typically be more efficient
then using `rows()` by a wider margin.

API
---

`groovy-sql-stream-extension` library in its `0.4.3` version supports the following functionalities:

```java
/*
 * withStream follows the groovy.sql.Sql API, with the
 * exception being offset and maxRow parameters, which
 * can be emulated with methods drop(offset), take(maxRow)
 */

// executing the query

Sql.withStream(String, Closure)
Sql.withStream(String, Closure, Closure)
Sql.withStream(GString, Closure)
Sql.withStream(GString, Closure, Closure)
Sql.withStream(Map, String, Closure)
Sql.withStream(Map, String, Closure, Closure)
Sql.withStream(String, List, Closure)
Sql.withStream(String, List, Closure, Closure)
Sql.withStream(String, Map, Closure)
Sql.withStream(String, Map, Closure, Closure)

// traversing and processing the data set

StreamingResultSet.collect(Closure)
StreamingResultSet.collectMany(Closure)
StreamingResultSet.findAll(Closure<Boolean>)
StreamingResultSet.each(Closure)
StreamingResultSet.take(int)
StreamingResultSet.takeWhile(Closure<Boolean>)
StreamingResultSet.drop(int)
StreamingResultSet.dropWhile(Closure<Boolean>)

// forcing the realization of the stream

StreamingResultSet.force()
StreamingResultSet.toList()
```

It is important to note that after `withStream()` method returns, the JDBC `ResultSet` will be closed.
If the stream is not by then realized by invoking `force()` or `toList()`, the data will not be
accessible (remember, the `ResultSet` is closed).

License
-------

Copyright 2013-2014 Dinko Srko&#0269;.
Released under the [Apache Public License, v2.0][2]

----

This work is made possible with support from [Helix d.o.o.][1]


[1]: http://www.helix.hr                        "Helix d.o.o"
[2]: http://www.apache.org/licenses/LICENSE-2.0 "Apache Public License, v2.0"
