/*
 * Copyright 2013-2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package hr.helix.sqlstream

import groovy.sql.Sql
import spock.lang.Specification

import java.sql.ResultSet

/**
 * Compares the performance of withSteam() vs Sql#rows() vs Sql#eachRow()
 *
 * @author Dinko Srkoč
 * @since 2014-01-29
 */
class WithStreamSpec extends Specification {

    private static n = 1000002
//    private static n = 42

    Sql sql
    def setup() {
        sql = Sql.newInstance('jdbc:h2:mem:', 'org.h2.Driver')
        sql.execute 'CREATE TABLE a_table (col_a INTEGER, col_b INTEGER, col_c INTEGER)'
        (1..n).collate(3).each {
            sql.execute('INSERT INTO a_table (col_a, col_b, col_c) VALUES (?, ?, ?)', it)
        }
    }

    private static <T> T time(String desc, Closure<T> act) {
        def start = System.nanoTime()
        try {
            act()
        } finally {
            println "TEST [$desc]: ${(System.nanoTime() - start) / 1e6} ms"
        }
    }

    @spock.lang.Ignore
    def 'warmup'() {
        given:
        def res = 0L

        when:
        time('warmup') { sql.eachRow('SELECT * FROM a_table') { res = it.col_a } }

        then:
        res == 1000000
    }

    def 'using Sql.rows()'() {
        when:
        List result = time('rows') {
            sql.rows('SELECT * FROM a_table').collectMany { row ->
                [row.col_a + row.col_b, row.col_c * 2] // [3, 6], [9, 12], [15, 18], [21, 24], ...
            }.collect {
                it < 10 ? it * 10 : it + 10            // 30, 60, 90, 22, 25, 28, 31, 34, ...
            }.findAll {
                it % 2 == 0                            // 30, 60, 90, 22, 28, 34, ...
            }
        }

        then:
        result.size() == n / 3 + 2
    }

    def 'using Sql.eachRow()'() {
        given:
        def result = []

        when:
        time('eachRow') {
            sql.eachRow('SELECT * FROM a_table') { row ->
                def x = row.col_a + row.col_b,
                    y = row.col_c * 2
                x = x < 10 ? x * 10 : x + 10
                y = y < 10 ? y * 10 : y + 10

                if (x % 2 == 0)
                    result << x
                if (y % 2 == 0)
                    result << y
            }
        }

        then:
        result.size() == n / 3 + 2
    }

    def 'using Sql.withStream()'() {
        when:
        List result = time('withStream') {
            sql.withStream('SELECT * FROM a_table') { StreamingResultSet stream ->
                stream.collectMany { row ->
                    [row.col_a + row.col_b, row.col_c * 2]
                }.collect {
                    it < 10 ? it * 10 : it + 10
                }.findAll {
                    it % 2 == 0
                }.toList()
            }
        }

        then:
        result.size() == n / 3 + 2
    }

    def 'test head()'() {
        when:
        sql.resultSetType = ResultSet.TYPE_SCROLL_INSENSITIVE // if we want to play with forcing the stream more than once
        def result = sql.withStream('SELECT * FROM a_table') { StreamingResultSet stream ->
            def s2 = stream.collect { it.col_a } // [1, 4, 7, 10, 13, 16, 19, 22, 25, ...]
            def h2 = s2.head()                   // 1
            assert s2.head() == h2
            def s3 = s2.tail()                   // [4, 7, 10, 13, 16, 19, 22, 25, ...]
            def h3 = s3.head()                   // 4
            def s4 = s3.drop(3)                  // [13, 16, 19, 22, 25, ...]
            def h4 = s4.head()                   // 13
            s4.take(2).toList() + h2 + h3 + h4
        }

        then:
        result == [13, 16, 1, 4, 13]
    }

    def 'test find'() {
        when:
        def result = sql.withStream('SELECT * FROM a_table') { StreamingResultSet stream ->
            stream.collect { it.col_a }.find { it == 1 }
        }

        then:
        result == 1
    }

    def 'test find - returning null if not found'() {
        when:
        def result = sql.withStream('SELECT * FROM a_table') { StreamingResultSet stream ->
            stream.collect { it.col_a }.find { it == -1 }
        }

        then:
        result == null
    }

    def 'test any even()'() {
        when:
        def result = sql.withStream('SELECT * FROM a_table') { StreamingResultSet stream ->
            stream.any {
                it.col_a % 2 == 0 // col_a has some even numbers: 1, 4, 7, 10, 13, 16, 19, ... (n=n+3 starting 1)
            }
        }

        then:
        result == true
    }

    def 'test any odd()'() {
        when:
        def result = sql.withStream('SELECT * FROM a_table') { StreamingResultSet stream ->
            stream.findAll {
                it.col_a % 2 == 1      // only get the odd numbers: 1, 7, 13, 19, 25, 31
            }.any {
                it.col_a % 2 == 0
            }
        }

        then:
        result == false
    }

    def 'test every even()'() {
        when:
        def result = sql.withStream('SELECT * FROM a_table') { StreamingResultSet stream ->
            stream.every {
                it.col_a % 2 == 0 // col_a has some even numbers: 1, 4, 7, 10, 13, 16, 19, ... (n=n+3 starting 1)
            }
        }

        then:
        result == false
    }

    def 'test every odd()'() {
        when:
        def result = sql.withStream('SELECT * FROM a_table') { StreamingResultSet stream ->
            stream.findAll {
                it.col_a % 2 == 1      // only get the odd numbers: 1, 7, 13, 19, 25, 31
            }.every {
                it.col_a % 2 == 1
            }
        }

        then:
        result == true
    }

    def 'test every with and empty stream must return true()'() {
        // That's because the Groovy GDK every() method returns true if the collections is empty (predicated is never evaluated)
        when:
        def result = sql.withStream('SELECT * FROM a_table') { StreamingResultSet stream ->
            stream.findAll {
                false        // gets an empty strem
            }.every {
                false        // it doens'nt matter
            }
        }

        then:
        result == true
    }

    def 'test contains all()'(List<Integer> items, boolean expected) {
        when:
        def result = sql.withStream('SELECT * FROM a_table limit 5') { StreamingResultSet stream ->
            stream.collect {
                it.col_a                   //  [1, 4, 7, 10, 13]
            }.containsAll(items)
        }

        then:
        result == expected

        where:
        items         | expected
        [4,7,10]      | true
        [4,7,10,99]   | false
        []            | true
    }

    def "test contains all doesn't modify the items collection()"(items) {
        // The containsAll method remove every item contained in the stream, so the items collection shouldn't be modified
        when:
        sql.withStream('SELECT * FROM a_table limit 5') { StreamingResultSet stream ->
            stream.collect {
                it.col_a                   //  [1, 4, 7, 10, 13]
            }.containsAll(items)
        }

        then:
        items.size() == old(items.size())

        where:
        items = [1, 4, 7]

    }

    def 'test contains all with an empty stream must return false()'() {
        when:
        def result = sql.withStream('SELECT * FROM a_table limit 5') { StreamingResultSet stream ->
            stream.findAll {
                false
            }.containsAll([9999,0])
        }

        then:
        result == false

    }
}
