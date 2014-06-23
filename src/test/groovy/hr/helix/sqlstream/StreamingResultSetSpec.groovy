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

import hr.helix.sqlstream.StreamingResultSet as SRR

import spock.lang.Specification
import spock.lang.Unroll

import java.sql.ResultSet

/**
 * 
 * @author dsrkoc
 * @since 2013-10-31
 */
@SuppressWarnings("GroovyAccessibility")
class StreamingResultSetSpec extends Specification {

    def calc = new SRR.Fn()

    @spock.lang.Ignore
    def 'test the paths'() {
        given:
        def rs = Stub(ResultSet) {
            receive('next') >>> [true, true, true, true, false]
            receive('getObject') >>> [1, 2, 3, 4, 5]
        }
        def streamedRs = SRR.from(rs)

        when:
        def res = streamedRs.collectMany { println "TEST: value=$it"; [it * it] }.force()

        then:
        res.toList() == [1, 4, 9, 16]
    }

    def 'test collectMany'() {
        given:
        def fn = calc.andThen(new SRR.CollectMany<List>({ [it * it] }))
        def v = new SRR.Value(2)
        def lst = []

        when:
        def res = fn.call(v)
        res.exportTo(lst)

        then:
//        res.getStat()  == SRR.Status.OK // ****
        assert res instanceof SRR.Value // **** not good enough as Value can be subclassed
        lst == [4]
    }

    def 'test collect'() {
        def fn = calc.andThen(new SRR.Collect<Integer>({ it * it }))
        def input    = [1, 2, 3]
        def expected = [1, 4, 9]

        expect:
        force(toVals(input), fn) == expected
    }

    def 'test take'() {
        given:
        def fn = calc.andThen(new SRR.Take(2))
        def input    = [1, 2, 3, 4]
        def expected = [1, 2]

        expect:
        force(toVals(input), fn) == expected
    }

    def 'test drop'() {
        given:
        def fn = calc.andThen(new SRR.Drop(2))
        def input    = [1, 2, 3, 4]
        def expected = [3, 4]

        expect:
        force(toVals(input), fn) == expected
    }

    def 'test takeWhile'() {
        given:
        def fn1 = new SRR.Fn().andThen(new SRR.TakeWhile({ it < 4 }))
        def fn2 = new SRR.Fn().andThen(new SRR.TakeWhile({ it % 2 == 0 }))
        def input     = [1, 2, 3, 4, 5, 6]
        def expected1 = [1, 2, 3]
        def expected2 = []

        expect:
        force(toVals(input), fn1) == expected1
        force(toVals(input), fn2) == expected2
    }

    def 'test dropWhile'() {
        given:
        def fn = calc.andThen(new SRR.DropWhile({ it < 4 }))
        def input    = [1, 2, 3, 4, 5, 6]
        def expected = [4, 5, 6]

        expect:
        force(toVals(input), fn) == expected
    }

    def 'test findAll'() {
        given:
        def fn = calc.andThen(new SRR.FindAll({ it % 2 == 0 }))
        def input    = [1, 2, 3, 4, 5, 6]
        def expected = [2, 4, 6]

        expect:
        force(toVals(input), fn) == expected
    }

    def 'test each'() {
        given:
        def lst = []
        def fn = calc.andThen(new SRR.Each({ lst << (it + 1) }))
        def input    = [1, 2, 3, 4]
        def expected = [2, 3, 4, 5]

        when:
        force(toVals(input), fn)

        then:
        lst == expected
    }

    def 'test unique'() {
        given:
        def fn = calc.andThen(new SRR.Unique())
        def input    = [1, 1, 2, 3, 3, 1]
        def expected = [1, 2, 3]

        expect:
        force(toVals(input), fn) == expected
    }

    def 'test collect.collectMany.take'() {
        given:
        def fn = calc.andThen(new SRR.Collect<Integer>({ it + 1 }))
                     .andThen(new SRR.CollectMany<List<Integer>>({ [it * 2] }))
                     .andThen(new SRR.Take(3));
        def input    = [1, 2, 3, 4, 5, 6]
        def expected = [4, 6, 8]

        expect:
        force(toVals(input), fn) == expected
    }

    def 'test collectMany.collectMany.take'() {
        given:
        def fn = calc.andThen(new SRR.CollectMany<List<Integer>>({ [it.a, it.b] }))
                     .andThen(new SRR.CollectMany<List<Integer>>({ [it] }))
                     .andThen(new SRR.Take(5))
        def input    = [[a: 1, b: 2], [a: 3, b: 4], [a: 5, b: 6], [a: 7, b: 8], [a: 9, b: 10]]
        def expected = [1, 2, 3, 4, 5]

        expect:
        force(toVals(input), fn) == expected
    }

    def 'test collectMany.findAll.take'() {
        given:
        def fn = calc.andThen(new SRR.CollectMany<List<Integer>>({ [it.a, it.b] }))
                     .andThen(new SRR.FindAll({ it % 2 == 0 }))
                     .andThen(new SRR.Take(3))
        def input    = [[a: 1, b: 2], [a: 3, b: 4], [a: 5, b: 6], [a: 7, b: 8], [a: 9, b: 10]]
        def expected = [2, 4, 6]

        expect:
        force(toVals(input), fn) == expected
    }

    def 'test take.drop.take.drop'() {
        given:
        def fn = calc.andThen(new SRR.Take(6)) // 1..6
                     .andThen(new SRR.Drop(1)) // 2..6
                     .andThen(new SRR.Take(4)) // 2..5
                     .andThen(new SRR.Drop(2)) // 4..5
        def input    = 1..10
        def expected = [4, 5]

        expect:
        force(toVals(input), fn) == expected
    }

    def 'test takeWhile.collect.dropWhile'() {
        given:
        def fn = calc.andThen(new SRR.TakeWhile({ it < 8 }))              //  1 .. 7
                     .andThen(new SRR.Collect<String>({ it.toString() })) // '1'..'7'
                     .andThen(new SRR.DropWhile({ it < '4' }))            // '4'..'7'
        def input    = 1..10
        def expected = '4'..'7'

        expect:
        force(toVals(input), fn) == expected
    }

    def 'test head'() {
        given:
        def fn = calc.andThen(new SRR.Head())
        def input = 1..10
        def expected = [1]

        expect:
        force(toVals(input), fn) == expected
    }

    def 'test collect.drop.head'() {
        given:
        def fn = calc.andThen(new SRR.Collect<String>({ it.toString() }))
                     .andThen(new SRR.Drop(3))
                     .andThen(new SRR.Head())
        def input = 1..10
        def expected = ['4']

        expect:
        force(toVals(input), fn) == expected
    }

    def 'test find'() {
        given:
        def fn = calc.andThen(new SRR.Find({it == 2}))
        def input = 1..10
        def expected = [2]

        expect:
        force(toVals(input), fn) == expected
    }

    def 'test collect.drop.find'() {
        given:
        def fn = calc.andThen(new SRR.Collect<String>({ it.toString() }))
                     .andThen(new SRR.Drop(3))
                     .andThen(new SRR.Find({it == '6'}))
        def input = 1..10
        def expected = ['6']

        expect:
        force(toVals(input), fn) == expected
    }

    def 'test collect.find - returning null if element not found'() {
        given:
        def fn = calc.andThen(new SRR.Collect<String>({ it.toString() }))
                     .andThen(new SRR.Find({it == 'xxx'}))
        def input = 1..10
        def expected = [] // todo should be null, probably have to change to `force()`

        expect:
        force(toVals(input), fn) == expected
    }

    @Unroll
    def 'test any'(List<Integer> input, List<Boolean> expected) {
        given:
        def fn = calc.andThen(new SRR.Any({ it % 2 == 0 }))

        expect:
        force(toVals(input), fn) == expected

        where:
        input     | expected
        []        | []
        [5, 2, 7] | [true]
        [5, 2]    | [true]
        [2, 5]    | [true]
        [1, 3]    | []
    }

    @Unroll
    def 'test every'(List<Integer> input, List<Boolean> expected) {
        given:
        def fn = calc.andThen(new SRR.Every({ it % 2 == 0 }))

        expect:
        force(toVals(input), fn) == expected

        where:
        input      | expected
        []         | []          // it return empty (means false) but the every(), but the method returs true
        [2, 2, 2]  | []
        [2, 2, 1]  | [false]
        [2, 1, 2]  | [false]
        [1, 2, 2]  | [false]
        [1, 1, 2]  | [false]
        [1, 2, 1]  | [false]
        [2, 1, 1]  | [false]
        [1, 2]     | [false]
        [2, 1]     | [false]
        [2]        | []
        [1]        | [false]
        [1, 1]     | [false]
        [1, 1, 1]  | [false]
    }

    def 'containsAll'(List<Integer> input, List<Integer> contains, List<Boolean> expected) {
        given:
        def fn = calc.andThen(new SRR.ContainsAll(contains))

        expect:
        force(toVals(input), fn) == expected

        where:
        input      | contains | expected
        [1, 2, 3]  | []       | [true]
        [1, 2, 3]  | [2]      | [true]
        [1, 2, 3]  | [1,2]    | [true]
        [1, 2, 3]  | [1,2,3]  | [true]
        [1, 2, 3]  | [1,2,3,4]| []
        [1, 2, 3]  | [1,2,4]  | []
        [1, 2, 3]  | [4]      | []
        [1, 2, 3]  | []       | [true]
    }

    private static List<SRR.Value> toVals(List xs) { xs.collect { new SRR.Value(it) }}

    // NOTE: implementation of this method should always be in sync with StreamingResultSet.force()
    private static List force(List<SRR.Value> svs, SRR.Fn fn) {
        def lst = []
        for (it in svs) {
            def v = fn.call(it)
            v.exportTo(lst)
            if (v.terminate())
                break
        }
        lst
    }
}
