package org.isaacmcfadyen.surrealjdbcdriver;

import org.json.JSONArray;
import org.json.JSONObject;

import java.sql.*;

public class SurrealStatement implements Statement {
    final private WebsocketConnection socket;
    final private SurrealConnection connection;
    final private String sql;
    final private String namespace;
    final private String schema;

    SurrealResultSet currentResultSet;

    public SurrealStatement(WebsocketConnection socket, SurrealConnection connection, String sql, String namespace, String schema) {
        this.socket = socket;
        this.connection = connection;
        this.sql = sql;
        this.namespace = namespace;
        this.schema = schema;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        // Make the actual query.
        JSONArray queryRequest = new JSONArray();
        queryRequest.put(sql);
        String queryId = socket.sendMessage("query", queryRequest);
        JSONObject results = socket.awaitResult(queryId);
        return new SurrealResultSet(
                results
                        .getJSONArray("result")
                        .getJSONObject(0)
                        .getJSONArray("result")
        );
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        // Make the actual query.
        JSONArray queryRequest = new JSONArray();
        queryRequest.put(sql);
        String queryId = socket.sendMessage("query", queryRequest);
        socket.awaitResult(queryId);
        return 0;
    }

    @Override
    public void close() throws SQLException {
        connection.close();
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return 0;
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {

    }

    @Override
    public int getMaxRows() throws SQLException {
        return 0;
    }

    @Override
    public void setMaxRows(int max) throws SQLException {

    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {

    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return 100;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {

    }

    @Override
    public void cancel() throws SQLException {

    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {

    }

    @Override
    public void setCursorName(String name) throws SQLException {

    }

    @Override
    public boolean execute(String sql) throws SQLException {
        // Bypass execute on keep alive because Surreal doesn't support that type of query.
        if(sql.contains("keep alive")) {
            return false;
        }

        JSONArray useRequest = new JSONArray();
        useRequest.put(namespace);
        useRequest.put(schema);
        String useId = socket.sendMessage("use", useRequest);
        socket.awaitResult(useId);

        // Make the actual query.
        JSONArray queryRequest = new JSONArray();
        queryRequest.put(sql);
        String queryId = socket.sendMessage("query", queryRequest);
        JSONObject results = socket.awaitResult(queryId);
        currentResultSet = new SurrealResultSet(
                results
                        .getJSONArray("result")
                        .getJSONObject(0)
                        .getJSONArray("result")
        );
        return true;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return currentResultSet;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        // TODO: This is basically an indicator. When a SELECT query updates some rows this will return
        // the number of rows updated, so the DB engine can requery to see the updates.
        // Because D1 doesn't return update count yet we just return -1 to everything.
        return -1;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return false;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {

    }

    @Override
    public int getFetchDirection() throws SQLException {
        return 0;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {

    }

    @Override
    public int getFetchSize() throws SQLException {
        return 0;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return 0;
    }

    @Override
    public int getResultSetType() throws SQLException {
        return 0;
    }

    @Override
    public void addBatch(String sql) throws SQLException {

    }

    @Override
    public void clearBatch() throws SQLException {

    }

    @Override
    public int[] executeBatch() throws SQLException {
        return new int[0];
    }

    @Override
    public Connection getConnection() throws SQLException {
        return null;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        return false;
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        return null;
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return 0;
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return 0;
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        return 0;
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        return false;
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        return false;
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        return false;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return 0;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return connection.isClosed();
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {

    }

    @Override
    public boolean isPoolable() throws SQLException {
        return false;
    }

    @Override
    public void closeOnCompletion() throws SQLException {

    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }
}
