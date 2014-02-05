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
import groovy.lang.GString;
import groovy.sql.Sql;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Extends {@code groovy.sql.Sql} class with methods that perform streaming
 * database queries. All methods are overloaded versions of {@code withStream()},
 * following other {@code Sql} methods such as {@code Sql#eachRow()}, or {@code Sql#rows()}.
 * The exception are methods with {@code offset} and {@code maxRows} parameters which
 * can be emulated with {@link StreamingResultSet#drop(int)} and {@link StreamingResultSet#take(int)}
 * respectively.
 * <p>
 * {@code withStream()} methods allow for efficient transformation of queried result set because
 * it accumulates the operations on the result set and evaluates them lazily. The elements of the
 * result set are iterated only once.
 * </p>
 *
 * <h4>Example</h4>
 *
 * Assuming {@code sql} is {@code groovy.sql.Sql} instance, and an {@code a_table}
 * table exists in the database.
 *
 * <pre>
 * sql.withStream('SELECT * FROM a_table') { stream ->
 *     stream.collect { row ->
 *         [ab: row.col_a + row._col_b, cc: calc(row.col_c)]
 *     }.findAll {
 *         it.cc in ['foo', 'bar', 'baz']
 *     }.takeWhile {
 *         it.ab > 100 && it.ab < 1000
 *     }.toList()
 * }
 * </pre>
 *
 * @author Dinko Srkoč
 * @since 2013-10-30
 */
public class SqlStreamExtension {

    /**
     * Performs the given SQL query calling the {@code streamClosure} with {@link StreamingResultSet}
     * instance.
     * <p>
     * Example usage:
     * </p>
     * <pre>
     * sql.withStream("SELECT * FROM a_table WHERE col_a LIKE 'D%'") { stream ->
     *     stream.collect {
     *         it.col_a.toLowerCase()
     *     }
     *     .toList()
     * }
     * </pre>
     *
     * Resource handling is performed automatically where appropriate.
     * <p>
     * <em>Note</em>: stream should be forced into realization by calling {@link StreamingResultSet#force()},
     * or {@link StreamingResultSet#toList()} before the end of the Closure block.
     * If stream is realised after {@code withStream()} closed the {@code ResultSet},
     * the {@code SQLException} with message <em>ResultSet not open...</em> will be thrown.
     * </p>
     *
     * @param self             object that is extended with this {@code withStream} method
     * @param sql              the sql query statement
     * @param streamClosure    called with {@link StreamingResultSet} instance
     * @param <T>              {@code streamClosure}'s return type
     * @return forwards the return value of {@code streamClosure} call
     * @throws SQLException if database access error occurs
     */
    public static <T> T withStream(Sql self, String sql, Closure<T> streamClosure) throws SQLException {
        return withStream(self, sql, (Closure) null, streamClosure);
    }

    /**
     * Performs the given SQL query calling the {@code streamClosure} with {@link StreamingResultSet}
     * instance. The {@code metaClosure} will be called once, passing in the {@code ResultSetMetaData}
     * as argument.
     * <p>
     * Example usage:
     * </p>
     * <pre>
     * def printColNames = { meta ->
     *     (1..meta.columnCount).each {
     *         print meta.getColumnLabel(it).padRight(20)
     *     }
     *     println()
     * }
     *
     * sql.withStream("SELECT * FROM a_table WHERE col_a LIKE 'D%'", printColNames) { stream ->
     *     stream.collect {
     *         it.col_a.toLowerCase()
     *     }
     *     .toList()
     * }
     * </pre>
     *
     * Resource handling is performed automatically where appropriate.
     * <p>
     * <em>Note</em>: stream should be forced into realization by calling {@link StreamingResultSet#force()},
     * or {@link StreamingResultSet#toList()} before the end of the Closure block.
     * If stream is realised after {@code withStream()} closed the {@code ResultSet},
     * the {@code SQLException} with message <em>ResultSet not open...</em> will be thrown.
     * </p>
     *
     * @param self             object that is extended with this {@code withStream} method
     * @param sql              the sql query statement
     * @param metaClosure      called for meta data (only once after sql execution)
     * @param streamClosure    called with {@link StreamingResultSet} instance
     * @param <T>              {@code streamClosure}'s return type
     * @return forwards the return value of {@code streamClosure} call
     * @throws SQLException if database access error occurs
     */
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

    /**
     * Performs the given SQL query calling the {@code streamClosure} with {@link StreamingResultSet}
     * instance. The query may contain {@code GString} expressions.
     * <p>
     * Example usage:
     * </p>
     * <pre>
     * def location = 25
     *
     * sql.withStream("SELECT * FROM a_table WHERE col_b < $location") { stream ->
     *     stream.collect {
     *         it.col_a.toLowerCase()
     *     }
     *     .toList()
     * }
     * </pre>
     *
     * Resource handling is performed automatically where appropriate.
     * <p>
     * <em>Note</em>: stream should be forced into realization by calling {@link StreamingResultSet#force()},
     * or {@link StreamingResultSet#toList()} before the end of the Closure block.
     * If stream is realised after {@code withStream()} closed the {@code ResultSet},
     * the {@code SQLException} with message <em>ResultSet not open...</em> will be thrown.
     * </p>
     *
     * @param self             object that is extended with this {@code withStream} method
     * @param sql              a {@code GString} containing the SQL query with embedded parameters
     * @param streamClosure    called with {@link StreamingResultSet} instance
     * @param <T>              {@code streamClosure}'s return type
     * @return forwards the return value of {@code streamClosure} call
     * @throws SQLException if database access error occurs
     */
    public static <T> T withStream(Sql self, GString sql, Closure<T> streamClosure) throws SQLException {
        return withStream(self, sql, null, streamClosure);
    }

    /**
     * Performs the given SQL query calling the {@code streamClosure} with {@link StreamingResultSet}
     * instance. The {@code metaClosure} will be called once, passing in the {@code ResultSetMetaData}
     * as argument. The query may contain {@code GString} expressions.
     * <p>
     * Example usage:
     * </p>
     * <pre>
     * def location = 25
     * def printColNames = { meta ->
     *     (1..meta.columnCount).each {
     *         print meta.getColumnLabel(it).padRight(20)
     *     }
     *     println()
     * }
     *
     * sql.withStream("SELECT * FROM a_table WHERE col_b < $location", printColNames) { stream ->
     *     stream.collect {
     *         it.col_a.toLowerCase()
     *     }
     *     .toList()
     * }
     * </pre>
     *
     * Resource handling is performed automatically where appropriate.
     * <p>
     * <em>Note</em>: stream should be forced into realization by calling {@link StreamingResultSet#force()},
     * or {@link StreamingResultSet#toList()} before the end of the Closure block.
     * If stream is realised after {@code withStream()} closed the {@code ResultSet},
     * the {@code SQLException} with message <em>ResultSet not open...</em> will be thrown.
     * </p>
     *
     * @param self             object that is extended with this {@code withStream} method
     * @param sql              a {@code GString} containing the SQL query with embedded parameters
     * @param streamClosure    called with {@link StreamingResultSet} instance
     * @param <T>              {@code streamClosure}'s return type
     * @return forwards the return value of {@code streamClosure} call
     * @throws SQLException if database access error occurs
     */
    public static <T> T withStream(Sql self, GString sql, Closure metaClosure, Closure<T> streamClosure) throws SQLException {
        SqlWrapper sqlWrapper = createWrapper(self);

        List params  = sqlWrapper.getParameters(sql);
        String query = sqlWrapper.asSql(sql, params);
        return withStream(self, query, params, metaClosure, streamClosure);
    }

    /**
     * A variant of {@link #withStream(groovy.sql.Sql, String, java.util.List, groovy.lang.Closure)} useful when
     * providing the named parameters as named arguments.
     *
     * @param self             object that is extended with this {@code withStream} method
     * @param params           a map of named parameters
     * @param sql              the sql query statement
     * @param streamClosure    called with {@link StreamingResultSet} instance
     * @param <T>              {@code streamClosure}'s return type
     * @return forwards the return value of {@code streamClosure} call
     * @throws SQLException if database access error occurs
     */
    public static <T> T withStream(Sql self, Map params, String sql, Closure<T> streamClosure) throws SQLException {
        return withStream(self, params, sql, null, streamClosure);
    }

    /**
     * A variant of {@link #withStream(groovy.sql.Sql, String, java.util.List, groovy.lang.Closure, groovy.lang.Closure)}
     * useful when providing the named parameters as named arguments.
     *
     * @param self             object that is extended with this {@code withStream} method
     * @param params           a map of named parameters
     * @param sql              the sql query statement
     * @param metaClosure      called for meta data (only once after sql execution)
     * @param streamClosure    called with {@link StreamingResultSet} instance
     * @param <T>              {@code streamClosure}'s return type
     * @return forwards the return value of {@code streamClosure} call
     * @throws SQLException if database access error occurs
     */
    public static <T> T withStream(Sql self, Map params, String sql, Closure metaClosure, Closure<T> streamClosure) throws SQLException {
        return withStream(self, sql, singletonList(params), metaClosure, streamClosure);
    }

    /**
     * Performs the given SQL query calling the {@code streamClosure} with {@link StreamingResultSet}
     * instance. The query may hold placeholder question marks which match the given list of parameters.
     * <p>
     * Example usage:
     * </p>
     * <pre>
     * sql.withStream("SELECT * FROM a_table WHERE col_a LIKE ?", ['%D']) { stream ->
     *     stream.collect {
     *         it.col_a.toLowerCase()
     *     }
     *     .toList()
     * }
     * </pre>
     *
     * Resource handling is performed automatically where appropriate.
     * <p>
     * <em>Note</em>: stream should be forced into realization by calling {@link StreamingResultSet#force()},
     * or {@link StreamingResultSet#toList()} before the end of the Closure block.
     * If stream is realised after {@code withStream()} closed the {@code ResultSet},
     * the {@code SQLException} with message <em>ResultSet not open...</em> will be thrown.
     * </p>
     *
     * @param self             object that is extended with this {@code withStream} method
     * @param sql              the sql query statement
     * @param params           a list of parameters
     * @param streamClosure    called with {@link StreamingResultSet} instance
     * @param <T>              {@code streamClosure}'s return type
     * @return forwards the return value of {@code streamClosure} call
     * @throws SQLException if database access error occurs
     */
    public static <T> T withStream(Sql self, String sql, List<Object> params, Closure<T> streamClosure) throws SQLException {
        return withStream(self, sql, params, null, streamClosure);
    }

    /**
     * Performs the given SQL query calling the {@code streamClosure} with {@link StreamingResultSet}
     * instance. The {@code metaClosure} will be called once, passing in the {@code ResultSetMetaData}
     * as argument. The query may hold placeholder question marks which match the given list of parameters.
     * <p>
     * Example usage:
     * </p>
     * <pre>
     * def printColNames = { meta ->
     *     (1..meta.columnCount).each {
     *         print meta.getColumnLabel(it).padRight(20)
     *     }
     *     println()
     * }
     *
     * sql.withStream("SELECT * FROM a_table WHERE col_a LIKE ?", ['%D'], printColNames) { stream ->
     *     stream.collect {
     *         it.col_a.toLowerCase()
     *     }
     *     .toList()
     * }
     * </pre>
     *
     * Resource handling is performed automatically where appropriate.
     * <p>
     * <em>Note</em>: stream should be forced into realization by calling {@link StreamingResultSet#force()},
     * or {@link StreamingResultSet#toList()} before the end of the Closure block.
     * If stream is realised after {@code withStream()} closed the {@code ResultSet},
     * the {@code SQLException} with message <em>ResultSet not open...</em> will be thrown.
     * </p>
     *
     * @param self             object that is extended with this {@code withStream} method
     * @param sql              the sql query statement
     * @param params           a list of parameters
     * @param metaClosure      called for meta data (only once after sql execution)
     * @param streamClosure    called with {@link StreamingResultSet} instance
     * @param <T>              {@code streamClosure}'s return type
     * @return forwards the return value of {@code streamClosure} call
     * @throws SQLException if database access error occurs
     */
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

    /**
     * A variant of {@link #withStream(groovy.sql.Sql, String, java.util.List, groovy.lang.Closure)} useful when
     * providing the named parameters as named arguments.
     *
     * @param self             object that is extended with this {@code withStream} method
     * @param sql              the sql query statement
     * @param params           a map of named parameters
     * @param streamClosure    called with {@link StreamingResultSet} instance
     * @param <T>              {@code streamClosure}'s return type
     * @return forwards the return value of {@code streamClosure} call
     * @throws SQLException if database access error occurs
     */
    public static <T> T withStream(Sql self, String sql, Map params, Closure<T> streamClosure) throws SQLException {
        return withStream(self, sql, params, null, streamClosure);
    }

    /**
     * A variant of {@link #withStream(groovy.sql.Sql, String, java.util.List, groovy.lang.Closure, groovy.lang.Closure)}
     * useful when providing the named parameters as named arguments.
     *
     * @param self             object that is extended with this {@code withStream} method
     * @param sql              the sql query statement
     * @param params           a map of named parameters
     * @param metaClosure      called for meta data (only once after sql execution)
     * @param streamClosure    called with {@link StreamingResultSet} instance
     * @param <T>              {@code streamClosure}'s return type
     * @return forwards the return value of {@code streamClosure} call
     * @throws SQLException if database access error occurs
     */
    public static <T> T withStream(Sql self, String sql, Map params, Closure metaClosure, Closure<T> streamClosure) throws SQLException {
        return withStream(self, sql, singletonList(params), metaClosure, streamClosure);
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

    private static ArrayList<Object> singletonList(Object item) { // lifted from groovy.sql.Sql#singletonList(Object)
        ArrayList<Object> params = new ArrayList<Object>();
        params.add(item);
        return params;
    }

}
