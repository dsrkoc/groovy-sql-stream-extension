package hr.helix.sqlstream;

import groovy.lang.Closure;
import groovy.sql.GroovyResultSet;
import groovy.sql.GroovyResultSetProxy;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class StreamingResultSet {
    private ResultSet rs;
    private Fn compute;
    private List<Object> values;

    private static class Fn {
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
                return new TerminateEmpty();
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

    // todo proučiti mogućnost generifikacije Value

    private static class Value {
        private Object value;

        public Object getValue() { return value; }

        protected Value() {}
        public Value(Object val) { value = val; }

        public void exportTo(List<Object> xs) { xs.add(value); }

        public boolean terminate() { return false; }
    }

    private static class FlatValue extends Value {
        private final Collection<Object> values;

        public FlatValue(List<Object> vals) { values = vals; }

        @Override public void exportTo(List<Object> xs) { xs.addAll(values); }
    }

    private static class IgnoreValue extends Value {
        public static final IgnoreValue INSTANCE = new IgnoreValue();

        @Override public void exportTo(List<Object> xs) {}
    }

    private static class TerminateEmpty extends IgnoreValue {
        public static final TerminateEmpty INSTANCE = new TerminateEmpty();

        @Override public boolean terminate() { return true; }
    }

    private static class TerminateWithValue extends Value {
        private final Value v;

        public TerminateWithValue(Value v) { this.v = v; }

        @Override public boolean terminate() { return true; }
        @Override public void exportTo(List<Object> xs) { v.exportTo(xs); }
    }

    private StreamingResultSet(ResultSet rs, Fn fn) {
        this.rs = rs;
        this.compute = fn;
    }

    public static StreamingResultSet from(ResultSet rs) {
        return new StreamingResultSet(rs, new Fn());
    }

    public <T> StreamingResultSet collect(Closure<T> f)  { return next(new Collect<T>(f)); }

    public <T extends Collection> StreamingResultSet collectMany(Closure<T> f) { return next(new CollectMany<T>(f)); }

    public StreamingResultSet findAll(Closure<Boolean> p) { return next(new FindAll(p)); }

    /* def find(Closure<Boolean> p) { */
    /*     // ??? */
    /* } */

    public StreamingResultSet each(Closure<Object> f) { return next(new Each(f)); }

    public StreamingResultSet take(int n) { return next(new Take(n)); }

    public StreamingResultSet drop(int n) { return next(new Drop(n)); }

    public StreamingResultSet takeWhile(Closure<Boolean> p) { return next(new TakeWhile(p)); }

    public StreamingResultSet dropWhile(Closure<Boolean> p) { return next(new DropWhile(p)); }

    private StreamingResultSet next(Fn that) {
        return new StreamingResultSet(rs, compute.andThen(that));
    }

    public StreamingResultSet force() throws SQLException {
        if (values != null) return this; // stream is already realized

        values = new ArrayList<Object>();
        GroovyResultSet groovyRS = new GroovyResultSetProxy(rs).getImpl();
        while (groovyRS.next()) {
            Value v = compute.call(new Value(groovyRS));

            v.exportTo(values);
            if (v.terminate()) break;
        }

        return this;
    }

    public List<Object> toList() throws SQLException {
        return force().values;
    }
}
