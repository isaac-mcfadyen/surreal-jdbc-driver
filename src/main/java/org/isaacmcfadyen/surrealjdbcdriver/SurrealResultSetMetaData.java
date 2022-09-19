package org.isaacmcfadyen.surrealjdbcdriver;

import org.json.JSONArray;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;

public class SurrealResultSetMetaData implements ResultSetMetaData {
    HashMap<String, SurrealColumn> columns;

    public SurrealResultSetMetaData(HashMap<String, SurrealColumn> columns) {
        this.columns = columns;
    }

    @Override
    public int getColumnCount() throws SQLException {
        return columns.keySet().toArray().length;
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        return false;
    }

    @Override
    public int isNullable(int column) throws SQLException {
        return columnNullable;
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        return false;
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        return 3;
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return columns.keySet().toArray()[column - 1].toString();
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        return columns.keySet().toArray()[column - 1].toString();
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        return null;
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        return 0;
    }

    @Override
    public int getScale(int column) throws SQLException {
        return 0;
    }

    @Override
    public String getTableName(int column) throws SQLException {
        return null;
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        return null;
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        return columns.get(columns.keySet().toArray()[column - 1].toString()).getSqlType();
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        return columns.get(columns.keySet().toArray()[column - 1].toString()).getSqlTypeName();
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return true;
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        return null;
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
