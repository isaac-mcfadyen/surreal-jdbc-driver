package org.isaacmcfadyen.surrealjdbcdriver;

import org.json.JSONObject;

import java.sql.Types;

public class SurrealColumn {
    public String name;
    public Class<?> type;
    public boolean isNullable;

    public int getSqlType() {
        if(type == String.class) {
            return Types.VARCHAR;
        } else if(type == Integer.class) {
            return Types.INTEGER;
        } else if(type == Double.class) {
            return Types.DOUBLE;
        } else if(type == Boolean.class) {
            return Types.BOOLEAN;
        } else if(type == JSONObject.class){
            return Types.JAVA_OBJECT;
        } else {
            return Types.OTHER;
        }
    }
    public String getSqlTypeName() {
        if(type == String.class) {
            return "String";
        } else if(type == Integer.class) {
            return "Integer";
        } else if(type == Double.class) {
            return "Double";
        } else if(type == Boolean.class) {
            return "Boolean";
        } else if(type == JSONObject.class){
            return "Object";
        } else {
            return "Other";
        }
    }
}
