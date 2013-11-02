package hr.helix.sqlstream;

import groovy.lang.Closure;
import groovy.lang.GString;
import groovy.sql.Sql;

import java.sql.*;
import java.util.List;
import java.util.Map;

/**
 * Extends {@code groovy.sql.Sql} class with methods that perform streaming
 * database queries.
 *
 * @author Dinko Srkoč
 * @since 2013-10-30
 */
public class SqlStreamExtension {

    /**
     * todo withStream's description
     *
     * @param self             object that is extended with this {@code withStream} method
     * @param sql              the sql query statement
     * @param streamClosure    called with {@code StreamingResultSet} instance
     * @param <T>              {@code streamClosure}'s return type
     * @return forwards the return value of {@code streamClosure} call
     * @throws SQLException if database access error occurs
     */
    public static <T> T withStream(Sql self, String sql, Closure<T> streamClosure) throws SQLException {
        return withStream(self, sql, (Closure) null, streamClosure);
    }

    // maxRows is not needed, stream.take(x) can be used instead
    // ditto for offset and stream.drop(x)
    public static <T> T withStream(Sql self, String sql, Closure metaClosure, Closure<T> streamClosure) throws SQLException {
        SqlWrapper sqlWrapper = createWrapper(self);

        Connection connection = sqlWrapper.createConnection();
        Statement statement = sqlWrapper.getStatement(connection, sql);
        ResultSet results = null;
        try {
            results = statement.executeQuery(sql);

            if (metaClosure != null) metaClosure.call(results.getMetaData());

            return streamClosure.call(StreamingResultSet.from(results));
        } catch (SQLException e) {
            SqlWrapper.LOG.warning("Failed to execute: " + sql + " because: " + e.getMessage());
            throw e;
        } finally {
            sqlWrapper.closeResources(connection, statement, results);
        }
    }

    public static <T> T withStream(Sql self, GString sql, Closure<T> streamClosure) throws SQLException {
        return withStream(self, sql, null, streamClosure);
    }

    public static <T> T withStream(Sql self, GString sql, Closure metaClosure, Closure<T> streamClosure) throws SQLException {
        SqlWrapper sqlWrapper = createWrapper(self);

        List params  = sqlWrapper.getParameters(sql);
        String query = sqlWrapper.asSql(sql, params);
        return withStream(self, query, params, metaClosure, streamClosure);
    }

    public static <T> T withStream(Sql self, Map params, String sql, Closure<T> streamClosure) throws SQLException {
        return withStream(self, params, sql, null, streamClosure);
    }

    public static <T> T withStream(Sql self, Map params, String sql, Closure metaClosure, Closure<T> streamClosure) throws SQLException {
        return withStream(self, sql, createWrapper(self).singletonList(params), metaClosure, streamClosure);
    }

    public static <T> T withStream(Sql self, String sql, List<Object> params, Closure<T> streamClosure) throws SQLException {
        return withStream(self, sql, params, null, streamClosure);
    }

    public static <T> T withStream(Sql self, String sql, List<?> params, Closure metaClosure, Closure<T> streamClosure) throws SQLException {
        SqlWrapper sqlWrapper = createWrapper(self);

        Connection connection = sqlWrapper.createConnection();
        PreparedStatement statement = null;
        ResultSet results = null;
        try {
            statement = sqlWrapper.getPreparedStatement(connection, sql, params);
            results = statement.executeQuery();

            if (metaClosure != null) metaClosure.call(results.getMetaData());

            return streamClosure.call(StreamingResultSet.from(results));
        } catch (SQLException e) {
            SqlWrapper.LOG.warning("Failed to execute: " + sql + " because: " + e.getMessage());
            throw e;
        } finally {
            sqlWrapper.closeResources(connection, statement, results);
        }
    }

    public static <T> T withStream(Sql self, String sql, Map params, Closure<T> streamClosure) throws SQLException {
        return withStream(self, sql, params, null, streamClosure);
    }

    public static <T> T withStream(Sql self, String sql, Map params, Closure metaClosure, Closure<T> streamClosure) throws SQLException {
        return withStream(self, sql, createWrapper(self).singletonList(params), metaClosure, streamClosure);
    }

    private static SqlWrapper createWrapper(Sql sql) {
        SqlWrapper sqlWrapper;
        try {
            sqlWrapper = SqlWrapper.wrap(sql);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e); // indicates a bug in SqlWrapper initialization, not expected to happen
        }
        return sqlWrapper;
    }

}
