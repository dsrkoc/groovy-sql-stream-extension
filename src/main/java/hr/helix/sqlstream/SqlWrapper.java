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

import groovy.lang.GString;
import groovy.sql.Sql;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.*;
import java.util.List;
import java.util.logging.Logger;

/**
 * Allows access to {@code groovy.sql.Sql}'s protected/private members.
 * This is necessary because {@link SqlStreamExtension} could not otherwise
 * properly extend {@code Sql} class.
 *
 * @author Dinko Srkoč
 * @since 2013-10-30
 */
public class SqlWrapper {
    public static final Logger LOG = Logger.getLogger(Sql.class.getName());

    private static Method createConnection;
    private static Method getStatement;
    private static Method getPreparedStatement;
    private static Method closeResources;
    private static Method getParameters;
    private static Method asSql;

    private static NoSuchMethodException initializationException = null;

    private Sql sql;

    static {
        try {
            createConnection     = Sql.class.getDeclaredMethod("createConnection");
            getStatement         = Sql.class.getDeclaredMethod("getStatement", Connection.class, String.class);
            getPreparedStatement = Sql.class.getDeclaredMethod("getPreparedStatement", Connection.class, String.class, List.class);
            closeResources       = Sql.class.getDeclaredMethod("closeResources", Connection.class, Statement.class, ResultSet.class);
            getParameters        = Sql.class.getDeclaredMethod("getParameters", GString.class);
            asSql                = Sql.class.getDeclaredMethod("asSql", GString.class, List.class);

            createConnection.setAccessible(true);
            getStatement.setAccessible(true);
            getPreparedStatement.setAccessible(true);
            closeResources.setAccessible(true);
            getParameters.setAccessible(true);
            asSql.setAccessible(true);
        } catch (NoSuchMethodException e) {
            initializationException = e; // if any problem arises, store it until SqlWrapper is used
        }
    }

    private SqlWrapper(Sql sql) { this.sql = sql; }

    public static SqlWrapper wrap(Sql sql) throws NoSuchMethodException {
        if (initializationException != null) throw initializationException;
        return new SqlWrapper(sql);
    }

    public Connection createConnection() throws SQLException {
        return (Connection) invokeEx(createConnection);
    }

    public Statement getStatement(Connection connection, String query) throws SQLException {
        return (Statement) invokeEx(getStatement, connection, query);
    }

    public void closeResources(Connection connection, Statement statement, ResultSet results) {
        invoke(closeResources, connection, statement, results);
    }

    public List<?> getParameters(GString gstring) {
        return (List) invoke(getParameters, gstring);
    }

    public String asSql(GString gString, List<?> values) {
        return (String) invoke(asSql, gString, values);
    }

    public PreparedStatement getPreparedStatement(Connection connection, String query, List<?> params) throws SQLException {
        return (PreparedStatement) invokeEx(getPreparedStatement, connection, query, params);
    }

    private Object invoke(Method method, Object... params) {
        try {
            return method.invoke(sql, params);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e); // not expected to happen
        } catch (InvocationTargetException e) {
            throw new RuntimeException(e); // not expected to happen
        }
    }

    private Object invokeEx(Method method, Object... params) throws SQLException {
        try {
            return method.invoke(sql, params);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);     // not expected to happen
        } catch (InvocationTargetException e) {
            throw (SQLException) e.getCause(); // Sql#`method`() probably throws only SQLException
        }
    }
}
