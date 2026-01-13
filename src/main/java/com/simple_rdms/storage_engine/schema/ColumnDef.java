package com.simple_rdms.storage_engine.schema;

/**
 * Define the column properties such as name, and the datatype
 */
public class ColumnDef {
    private final String name;
    private final ColumnType type;

    public ColumnDef(String name, ColumnType type) {
        this.name = name;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public ColumnType getType() {
        return type;
    }
}
