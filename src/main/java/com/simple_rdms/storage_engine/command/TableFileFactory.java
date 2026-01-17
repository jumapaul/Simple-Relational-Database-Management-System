package com.simple_rdms.storage_engine.command;

import com.simple_rdms.storage_engine.disk_manager.TableFile;
import com.simple_rdms.storage_engine.schema.TableSchema;
import org.springframework.stereotype.Service;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class TableFileFactory {

    // Store all created tables
    private final Map<String, TableFile> tables = new ConcurrentHashMap<>();

    // Store schemas separately for easy access
    private final Map<String, TableSchema> schemas = new ConcurrentHashMap<>();

    /**
     * Register a table that was created externally
     */
    public void registerTable(String tableName, TableFile tableFile, TableSchema schema) {
        if (tables.containsKey(tableName)) {
            throw new IllegalStateException("Table already exists: " + tableName);
        }
        tables.put(tableName, tableFile);
        schemas.put(tableName, schema);
    }

    /**
     * Create a new table with the given schema and path
     */
    public TableFile createTable(TableSchema schema, Path tablePath) throws IOException {
        String tableName = schema.getTableName();

        if (tables.containsKey(tableName)) {
            throw new IllegalStateException("Table already exists: " + tableName);
        }

        TableFile tableFile = new TableFile(schema, tablePath);
        tables.put(tableName, tableFile);
        schemas.put(tableName, schema);
        return tableFile;
    }

    /**
     * Get an existing table by name
     */
    public TableFile getTable(String tableName) {
        TableFile table = tables.get(tableName);
        if (table == null) {
            throw new IllegalArgumentException("Table not found: " + tableName);
        }
        return table;
    }

    /**
     * Get schema for a table
     */
    public TableSchema getSchema(String tableName) {
        TableSchema schema = schemas.get(tableName);
        if (schema == null) {
            throw new IllegalArgumentException("Table not found: " + tableName);
        }
        return schema;
    }
}