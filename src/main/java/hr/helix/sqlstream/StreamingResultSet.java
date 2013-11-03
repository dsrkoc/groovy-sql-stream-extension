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

        public StatVal call(StatVal sv) { return apply(sv); } // override this

        protected final StatVal apply(StatVal sv) { return next != null ? next.call(sv) : sv; }

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

        @Override public StatVal call(StatVal sv) {
            return apply(new StatVal(f.call(sv.getValue())));
        }
    }

    private static class CollectMany<T extends Collection> extends Fn {
        private Closure<T> f;

        public CollectMany(Closure<T> f) { this.f = f; }

        @Override public StatVal call(StatVal sv) {
            ArrayList<Object> vals = new ArrayList<Object>();
            for (Object x : f.call(sv.getValue())) {
                final StatVal _sv = apply(new StatVal(x));
                System.out.println("DEBUG: collectMany origval=" + sv.getValue() + ", newval=" + x + ", val=" + _sv.getValue() + ", status=" + _sv.getStat());
                if (_sv.getStat() == Status.OK) _sv.exportTo(vals);
                if (_sv.getStat() == Status.STOP_ITER) break;
/*
                switch (_sv.getStat()) {
                    case OK       : _sv.exportTo(vals); break;
                    case STOP_ITER: return _sv;
                }
*/
            }

            System.out.println("       returned values=" + vals);
            return new FlatStatVal(vals);
        }
    }

    private static class FindAll extends Fn {
        private Closure<Boolean> p;

        public FindAll(Closure<Boolean> p) { this.p = p; }

        @Override public StatVal call(StatVal sv) {
            return p.call(sv.getValue()) ? apply(sv) : sv.putStat(Status.STOP_STEP);
        }
    }

    private static class Each extends Fn {
        private Closure<Object> f;
        
        public Each(Closure<Object> f) { this.f = f; }

        @Override public StatVal call(StatVal sv) {
            f.call(sv.getValue());
            return apply(sv);
        }
    }

    private static class Take extends Fn {
        private int n;

        public Take(int n) { this.n = n; }

        @Override public StatVal call(StatVal sv) {
            System.out.println("DEBUG: take n=" + n + ", status=" + sv.getStat() + ", value=" + sv.getValue());
            if (n == 0) {
                return sv.getStat() == Status.STOP_ITER ? sv : sv.putStat(Status.STOP_ITER);
            } else {
                --n;
                return apply(sv);
            }
        }
    }

    private static class Drop extends Fn {
        private int n;

        public Drop(int n) { this.n = n; }

        @Override public StatVal call(StatVal sv) {
            if (n == 0) {
                return apply(sv.getStat() == Status.STOP_STEP ? sv.putStat(Status.OK) : sv);
            } else {
                --n;
                return sv.getStat() == Status.OK ? sv.putStat(Status.STOP_STEP) : sv;
            }
        }
    }

    // todo proučiti mogućnost generifikacije StatVala

    private static class StatVal {
        private Status stat = Status.OK;
        private Object value;

        public Object getValue() { return value; }
        public Status getStat()  { return stat;  }

        protected StatVal() {}
        public StatVal(Object val) { value = val; }

        public StatVal putStat(Status s)  { stat = s; return this; }

        public void exportTo(List<Object> xs) { xs.add(value); }
    }

    private static class FlatStatVal extends StatVal {
        private final Collection<Object> values;

        public FlatStatVal(List<Object> vals) { values = vals; }

        @Override public void exportTo(List<Object> xs) { xs.addAll(values); }
    }

    private enum Status { STOP_STEP, STOP_ITER, OK }

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

    private StreamingResultSet next(Fn that) {
        return new StreamingResultSet(rs, compute.andThen(that));
    }

    /*
     *  a) collect ... a -> b   ; b >> xs        ; sljedeći element
     *  b) findAll ... a -> bool; bool ? a >> xs ; sljedeći element
     *??c) find    ... a -> bool; bool ? vrati a ; bool ? sljedeći element
     *  d) each    ... a -> unit;                ; sljedeći element
     *  f) take    ...          ; a >> xs        ; n > 0 ? sljedeći element
     *  
     *  a -> b, sačuvaj b?, sljedeći element?
     */
    public StreamingResultSet force() throws SQLException {
        if (values != null) return this; // stream is already realized

        values = new ArrayList<Object>();
        GroovyResultSet groovyRS = new GroovyResultSetProxy(rs).getImpl();
        while (groovyRS.next()) {
            StatVal sv = compute.call(new StatVal(groovyRS));

            if (sv.getStat() == Status.OK) sv.exportTo(values);
            if (sv.getStat() == Status.STOP_ITER) break;
        }

        return this;
    }

    public List<Object> toList() throws SQLException {
        return force().values;
    }
}
