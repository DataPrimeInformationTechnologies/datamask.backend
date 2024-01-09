package com.data.tools.api.constants;

public class SQLConst {

    public static String getSelectBySchemeAndTable(String schemaName, String tableName) {
        return String.format("SELECT * FROM %s.%s", schemaName, tableName);
    }

    public static String getTableDDLfromSourceDb(String schemaName,String tableName) {
        return String.format("SELECT DBMS_METADATA.GET_DDL('TABLE', '%s', '%s') FROM DUAL",tableName,schemaName);
    }
}
