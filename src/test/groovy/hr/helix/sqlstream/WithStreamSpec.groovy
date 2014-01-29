package hr.helix.sqlstream

import groovy.sql.Sql
import spock.lang.Specification

class WithStreamSpec extends Specification {

    Sql sql
    def setup() {
        sql = Sql.newInstance('jdbc:h2:mem:', 'org.h2.Driver')
        sql.execute 'CREATE TABLE a_table (col_a INTEGER, col_b INTEGER, col_c INTEGER)'
        (1..1000002).collate(3).each {
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
                [row.col_a + row.col_b, row.col_c * 2]
            }.collect {
                it < 10 ? it * 10 : it + 10
            }.findAll {
                it % 2 == 0
            }
        }

        then:
        result.size() == 333336
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
        result.size() == 333336
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
        result.size() == 333336
    }
}
