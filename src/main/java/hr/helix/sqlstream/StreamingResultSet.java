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
package hr.helix.sqlstream;

import groovy.lang.Closure;
import groovy.sql.GroovyResultSet;
import groovy.sql.GroovyResultSetProxy;
import org.codehaus.groovy.runtime.DefaultGroovyMethods;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * Wraps the {@code java.sql.ResultSet} and exposes some common collection methods
 * which are lazily evaluated. Iterates through the {@code ResultSet} only once,
 * invoking given methods for each row of the result set. The row will be a
 * {@code GroovyResultSet} which is a {@code ResultSet} that supports accessing the
 * fields using property style notation and ordinal index values.
 *
 * @author Dinko Srkoč
 * @author Alberto Vilches
 * @author Adam Sernheim
 * @since 2013-10-30
 */
public class StreamingResultSet {
    private ResultSet rs;
    private Fn compute;
    private List<Object> values;

    private static class Fn implements Cloneable {
        private Fn next;

        public Value call(Value v) { return apply(v); } // override this

        protected final Value apply(Value v) { return next != null ? next.call(v) : v; }

        public Fn andThen(Fn that) {
            if (next == null)
                next = that;
            else
                next.andThen(that);
            return this;
        }

        /** makes deep copy of this Fn object */
        @Override
        protected final Fn clone() {
            Fn cloned;
            try {
                cloned = (Fn) super.clone();
            } catch (CloneNotSupportedException e) {
                return null;
            }
            if (next != null) {
                cloned.next = next.clone();
            }
            return cloned;
        }
    }

    private static class Collect<T> extends Fn {
        private Closure<T> f;

        public Collect(Closure<T> f) { this.f = f; }

        @Override public Value call(Value v) {
            return apply(new Value(f.call(v.getValue())));
        }
    }

    private static class CollectMany<T extends Collection> extends Fn {
        private Closure<T> f;

        public CollectMany(Closure<T> f) { this.f = f; }

        @Override public Value call(Value v) {
            ArrayList<Object> vals = new ArrayList<Object>();

            for (Object o : f.call(v.getValue())) {
                final Value val = apply(new Value(o));
                if (val.terminate())
                    return new TerminateWithValue(new FlatValue(vals));
                val.exportTo(vals);
            }

            return new FlatValue(vals);
        }
    }

    private static class FindAll extends Fn {
        private Closure<Boolean> p;

        public FindAll(Closure<Boolean> p) { this.p = p; }

        @Override public Value call(Value v) {
            return p.call(v.getValue()) ? apply(v) : IgnoreValue.INSTANCE;
        }
    }

    private static class Find extends FindAll {
        public Find(Closure<Boolean> p) {
            super(p);
            andThen(new Head());
        }
    }

    private static class Any extends Fn {
        private Closure<Boolean> p;

        public Any(Closure<Boolean> p) { this.p = p; }

        @Override public Value call(Value v) {
            return p.call(v.getValue()) ? TerminateWithValue.TRUE : IgnoreValue.INSTANCE;
        }
    }

    private static class Every extends Fn {
        private Closure<Boolean> p;

        public Every(Closure<Boolean> p) { this.p = p; }

        @Override public Value call(Value v) {
            return p.call(v.getValue()) ? IgnoreValue.INSTANCE : TerminateWithValue.FALSE;
        }
    }

    private static class ContainsAll extends Fn {
        private Collection<?> items;

        public ContainsAll(Collection<?> items) {
            this.items = new ArrayList<Object>(items);
        }

        @Override
        public Value call(Value v) {
            if (items.contains(v.getValue())) {
                items.remove(v.getValue()); // Every stream item found in the item collection is removed
            }
            return items.isEmpty() ? TerminateWithValue.TRUE : IgnoreValue.INSTANCE;
        }
    }

    private static class Unique extends Fn {
        private Closure closure;
        private Set seenValues;

        public Unique(Closure closure) {
            this.closure = closure;
            seenValues = new HashSet();
        }

        @Override public Value call(Value v) {
            Object value = (closure == null) ? v.getValue() : closure.call(v.getValue());
            if(seenValues.contains(value)) {
                return IgnoreValue.INSTANCE;
            } else {
                seenValues.add(value);
                return apply(v);
            }
        }
    }

    private static class Each extends Fn {
        private Closure<Object> f;

        public Each(Closure<Object> f) { this.f = f; }

        @Override public Value call(Value v) {
            f.call(v.getValue());
            return apply(v);
        }
    }

    private static class Take extends Fn {
        private int n;

        public Take(int n) { this.n = n; }

        @Override public Value call(Value v) {
            if (n == 0) {
                return TerminateEmpty.INSTANCE;
            } else {
                --n;
                return apply(v);
            }
        }
    }

    private static class TakeWhile extends Fn {
        private Closure<Boolean> p;
        private boolean done = false;

        public TakeWhile(Closure<Boolean> p) { this.p = p; }

        @Override public Value call(Value v) {
            if (done || (done = !p.call(v.getValue()))) {
                return TerminateEmpty.INSTANCE;
            } else {
                return apply(v);
            }
        }
    }

    private static class Drop extends Fn {
        private int n;

        public Drop(int n) { this.n = n; }

        @Override public Value call(Value v) {
            if (n == 0) {
                return apply(v);
            } else {
                --n;
                return IgnoreValue.INSTANCE;
            }
        }
    }

    private static class DropWhile extends Fn {
        private Closure<Boolean> p;
        private boolean done = false;

        public DropWhile(Closure<Boolean> p) { this.p = p; }

        @Override public Value call(Value v) {
            if (done || (done = !p.call(v.getValue()))) {
                return apply(v);
            } else {
                return IgnoreValue.INSTANCE;
            }
        }
    }

    private static class Head extends Fn {

        @Override public Value call(Value v) {
            return v.ignore() ? v : new TerminateWithValue(v);
        }

    }

    // todo proučiti mogućnost generifikacije Value

    private static class Value {
        private Object value;

        public Object getValue() { return value; }

        protected Value() {}
        public Value(Object val) { value = val; }

        public void exportTo(List<Object> xs) { xs.add(value); }

        public boolean terminate() { return false; }

        public boolean ignore() { return false; }
    }

    private static class FlatValue extends Value {
        private final Collection<Object> values;

        public FlatValue(List<Object> vals) { values = vals; }

        @Override public void exportTo(List<Object> xs) { xs.addAll(values); }
    }

    private static class IgnoreValue extends Value {
        public static final IgnoreValue INSTANCE = new IgnoreValue();

        @Override public void exportTo(List<Object> xs) {}
        @Override public boolean ignore() { return true; }
    }

    private static class TerminateEmpty extends IgnoreValue {
        public static final TerminateEmpty INSTANCE = new TerminateEmpty();

        @Override public boolean terminate() { return true; }
    }

    private static class TerminateWithValue extends Value {
        private final Value v;

        public static final TerminateWithValue TRUE = new TerminateWithValue(new Value(Boolean.TRUE));
        public static final TerminateWithValue FALSE = new TerminateWithValue(new Value(Boolean.FALSE));

        public TerminateWithValue(Value v) { this.v = v; }

        @Override public boolean terminate() { return true; }
        @Override public boolean ignore() { return true; }
        @Override public void exportTo(List<Object> xs) { v.exportTo(xs); }
    }

    private StreamingResultSet(ResultSet rs, Fn fn) {
        this.rs = rs;
        this.compute = fn;
    }

    /**
     * Creates a new {@code StreamingResultSet} instance that wraps the {@code ResultSet}.
     *
     * @param rs ResultSet that is wrapped by the newly created StreamingResultSet
     * @return new instance of StreamingResultSet
     */
    public static StreamingResultSet from(ResultSet rs) {
        return new StreamingResultSet(rs, new Fn());
    }

    /**
     * Iterates through the stream transforming each element into a new value
     * using Closure {@code f}.
     *
     * @param f    the Closure used to transform each element of the stream
     * @return new {@code StreamingResultSet} instance
     */
    public <T> StreamingResultSet collect(Closure<T> f)  { return next(new Collect<T>(f)); }

    /**
     * Iterates through the stream transforming each element to a collection and
     * concatenates (flattens) the resulting collections into a single list.
     *
     * @param f      the Closure used to transform each element of the stream
     * @param <T>    the collection type that Closure {@code f} returns
     * @return new {@code StreamingResultSet} instance
     */
    public <T extends Collection> StreamingResultSet collectMany(Closure<T> f) { return next(new CollectMany<T>(f)); }

    /**
     * Finds all elements matching the given Closure predicate.
     *
     * @param p    the Closure that must evaluate to {@code true} for element to be taken
     * @return new {@code StreamingResultSet} instance
     */
    public StreamingResultSet findAll(Closure<Boolean> p) { return next(new FindAll(p)); }

    /**
     * Finds the first element matching the given Closure predicate.
     * The result is equivalent to {@code findAll} operation followed by {@code head}.
     *
     * @param p   the Closure that must evaluate to {@code true} for element to be taken
     * @return the first element matching the Closure predicate
     */
    public Object find(Closure<Boolean> p) throws SQLException {
        if (values != null)
            return DefaultGroovyMethods.find(values, p);

        return terminate(new Find(p), null);
    }

    /**
     * Iterates through the stream passing each element to the given Closure {@code f}.
     *
     * @param f the Closure applied to each element found
     * @return new {@code StreamingResultSet} instance
     */
    public StreamingResultSet each(Closure<Object> f) { return next(new Each(f)); }

    /**
     * Takes the first {@code n} elements from the head of the stream.
     *
     * @param n the number of elements to take from the stream
     * @return new {@code StreamingResultSet} instance
     */
    public StreamingResultSet take(int n) { return next(new Take(n)); }

    /**
     * Drops the given number of elements from the head of the stream if available.
     *
     * @param n    the number of elements to drop from the stream
     * @return new {@code StreamingResultSet} instance
     */
    public StreamingResultSet drop(int n) { return next(new Drop(n)); }

    /**
     * Takes the longest prefix of the stream where each element passed to the
     * given Closure predicate evaluates to {@code true}.
     *
     * @param p    the Closure that must evaluate to {@code true} to continue taking elements
     * @return new {@code StreamingResultSet} instance
     */
    public StreamingResultSet takeWhile(Closure<Boolean> p) { return next(new TakeWhile(p)); }

    /**
     * Returns a suffix of the stream where elements are dropped from the front while the
     * given Closure predicate evaluates to {@code true}.
     *
     * @param p    the predicate that must evaluate to {@code true} to continue dropping elements
     * @return new {@code StreamingResultSet} instance
     */
    public StreamingResultSet dropWhile(Closure<Boolean> p) { return next(new DropWhile(p)); }

    /**
     * Remove all duplicated items, using the default comparator.
     * Equals and hashCode need to be implemented.
     * Warning! This keeps all seen values, so it may take up a lot of memory.
     *
     * @return new {@code StreamingResultSet} instance
     */
    public StreamingResultSet unique() { return next(new Unique(null)); }

    /**
     * Like {@see unique}, but uses the result of the closure.
     * @param closure
     * @return
     */
    public StreamingResultSet unique(Closure closure) { return next(new Unique(closure)); }

    /**
     * Selects the first element of the stream.
     * <p>
     * <em>Note</em>: in order to read the first element {@code head()} has to start
     * processing the result set. {@code StreamingResultSet} can be used again, but,
     * currently, it will process the result set from the beginning. This means it will
     * have to return the cursor ({@code ResultSet#beforeFirst()}) before processing.
     * This will throw {@code SQLException} if the result set type is {@code TYPE_FORWARD_ONLY}.
     * </p>
     * <strong>Example</strong>
     * <pre>
     * // be careful about result set type if stream is forced more than once
     * sql.resultSetType = ResultSet.TYPE_SCROLL_INSENSITIVE 
     *
     * def result = sql.withStream('SELECT * FROM a_table') { StreamingResultSet stream ->
     *     // calls ResultSet#next() to read the first element
     *     def h  = stream.head()
     *
     *     // calls ResultSet#beforeFirst() to start processing from the beginning
     *     def xs = stream.collect { it.col_a }.toList()
     *     h + xs.sum()
     * }
     * </pre>
     *
     * @return the first element of this stream
     */
    public Object head() throws SQLException {
        if (values != null)
            return values.get(0);

        StreamingResultSet srs = new StreamingResultSet(rs, compute.clone().andThen(new Head())); //next(new Head());
        return srs.toList().get(0);
    }

    /**
     * Iterates over the stream and checks whether the predicate is valid for at least one element.
     *
     * <p><strong>Example</strong></p>
     * <pre>
     * def result = sql.withStream('SELECT * FROM a_table') { StreamingResultSet stream ->
     *     boolean atLeastOneEvenElement = stream.any {
     *         it.col_a % 2 == 0
     *     }
     * }
     * </pre>
     *
     * @param p    the Closure predicate that must evaluate to {@code true} at least once
     *             for this method to return {@code true}
     * @return true if any item in the stream matches the Closure predicate
     * @throws SQLException if database access error occurs
     */
    public boolean any(Closure<Boolean> p) throws SQLException {
        if (values != null)
            return DefaultGroovyMethods.any(values, p);

        return terminateBool(new Any(p), Boolean.FALSE);
    }

    /**
     * Iterates over the stream and check if the predicate is valid for all the elements
     * (i.e. returns {@code true} for all items in this data structure).
     *
     * <p><strong>Example</strong></p>
     * <pre>
     * def result = sql.withStream('SELECT * FROM a_table') { StreamingResultSet stream ->
     *     boolean areAllElementsEven = stream.every {
     *         it.col_a % 2 == 0
     *     }
     * }
     * </pre>
     *
     * @param p    the Closure predicate that must evaluate to {@code true} for each stream element
     *             for this method to return {@code true}
     * @return true if all items in the stream match the Closure predicate
     * @throws SQLException if database access error occurs
     */
    public boolean every(Closure<Boolean> p) throws SQLException {
        if (values != null)
            return DefaultGroovyMethods.every(values, p);

        return terminateBool(new Every(p), Boolean.TRUE);
    }

    /**
     * Checks if the stream contains all the elements in the specified array.
     *
     * <p><strong>Example</strong></p>
     * <pre>
     * def result = sql.withStream('SELECT * FROM a_table') { StreamingResultSet stream ->
     *     boolean areAllElementsEven = stream.collect {
     *         it.col_a
     *     }.containsAll([1,2,10])
     * }
     * </pre>
     *
     * @param items    array to be checked for containment in this stream.
     * @return true if the stream contains all the elements.
     * @throws SQLException if database access error occurs
     */
    public boolean containsAll(Object[] items) throws SQLException {
        return containsAll(Arrays.asList(items));
    }

    /**
     * Checks if the stream contains all the elements in the specified collection.
     *
     * @param items    collection to be checked for containment in this stream.
     * @return true if the stream contains all the elements.
     * @throws SQLException if database access error occurs
     */
    public boolean containsAll(Collection<?> items) throws SQLException {
        if (values != null)
            values.containsAll(items);

        return terminateBool(new ContainsAll(items), Boolean.FALSE);
    }

    /**
     * Selects all elements except the head of the stream.
     * <p>
     * This is an alias for {@code drop(1)}
     * </p>
     *
     * @return new {@code StreamingResultSet} instance with all the elements of
     *         this stream except the first one
     */
    public StreamingResultSet tail() { return drop(1); }

    private StreamingResultSet next(Fn that) {
        return new StreamingResultSet(rs, compute.andThen(that));
    }

    /**
     * Terminating functions should end with an empty stream or with a single element stream.
     * This method returns either the value from the stream or the default value if the stream is empty.
     *
     * @param that              terminating function that is invoked at the end of {@code compute} chain
     * @param defaultIfEmpty    default value to be returned if realized stream is empty
     * @return the first element of the realized stream or {@code defaultIfEmpty} if the stream is empty
     * @throws SQLException if database access error occurs
     */
    private Object terminate(Fn that, Object defaultIfEmpty) throws SQLException {
        StreamingResultSet srs = new StreamingResultSet(rs, compute.clone().andThen(that));

        List results = srs.toList();
        return results.isEmpty() ? defaultIfEmpty : results.get(0);
    }

    /**
     * Boolean terminating functions should end with an empty stream or with a single element stream
     * with a boolean value. This method returns either the boolean value from the stream or the default
     * value if the stream is empty.
     *
     * @param that              terminating function that is invoked at the end of {@code compute} chain
     * @param defaultIfEmpty    default boolean value to be returned if realized stream is empty
     * @return the first element of the realized stream or {@code defaultIfEmpty} if the stream is empty
     * @throws SQLException if database access error occurs
     */
    private Boolean terminateBool(Fn that, Boolean defaultIfEmpty) throws SQLException {
        return (Boolean) terminate(that, defaultIfEmpty);
    }

    /**
     * Realizes the stream if it is not yet realized. The stream is realized
     * by iterating over the result set and applying all the given operations.
     * The resulting entries are collected into a list which can be accessed
     * by applying {@link StreamingResultSet#toList()} to this {@code StreamingResultSet}.
     *
     * @return this {@code StreamingResultSet} that is forced into realization
     * @throws SQLException if database access error occurs
     */
    public StreamingResultSet force() throws SQLException {
        if (values != null) return this; // stream is already realized

        values = new ArrayList<Object>();
        GroovyResultSet groovyRS = new GroovyResultSetProxy(rs).getImpl();

        // if several instances of this stream are forced then each should start at the beginning
        // CAUTION: beforeFirst() throws SQLException if ResultSet type is TYPE_FORWARD_ONLY!
        if (!groovyRS.isBeforeFirst())
            groovyRS.beforeFirst();

        while (groovyRS.next()) {
            Value v = compute.call(new Value(groovyRS));

            v.exportTo(values);
            if (v.terminate()) break;
        }

        return this;
    }

    /**
     * Returns the list of values computed by the given transformations.
     * Forces the stream realization if needed.
     *
     * @return the list of computed values
     * @throws SQLException if database access error occurs
     * @see StreamingResultSet#force()
     */
    public List<Object> toList() throws SQLException {
        return force().values;
    }
}
