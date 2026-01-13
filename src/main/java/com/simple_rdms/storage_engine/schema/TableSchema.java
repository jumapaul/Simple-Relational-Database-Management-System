package com.simple_rdms.storage_engine.schema;

import java.util.List;

/**
 * Definition of table schema
 * A table should have a name, columns and primary index which is always 0;
 */

public class TableSchema {
    private final String tableName;
    private final List<ColumnDef> columns;
    private final int primaryKeyIndex;

    public TableSchema(String tableName, List<ColumnDef> columns, int primaryKeyIndex) {
        this.tableName = tableName;
        this.columns = columns;
        this.primaryKeyIndex = primaryKeyIndex;
    }

    public String getTableName() {
        return tableName;
    }

    public List<ColumnDef> getColumns() {
        return columns;
    }

    public int getPrimaryKeyIndex() {
        return primaryKeyIndex;
    }
}
