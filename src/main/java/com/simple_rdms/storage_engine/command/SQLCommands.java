package com.simple_rdms.storage_engine.command;

import com.simple_rdms.storage_engine.database_manager.DatabaseManager;
import com.simple_rdms.storage_engine.disk_manager.TableFile;
import com.simple_rdms.storage_engine.schema.ColumnDef;
import com.simple_rdms.storage_engine.schema.ColumnType;
import com.simple_rdms.storage_engine.schema.TableSchema;
import com.simple_rdms.storage_engine.sql_interface.SQLTableInterface;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

@ShellComponent
public class SQLCommands {

    private final DatabaseManager databaseManager;
    private final TableFileFactory tableFileFactory;

    public SQLCommands(DatabaseManager databaseManager, TableFileFactory tableFileFactory) {
        this.databaseManager = databaseManager;
        this.tableFileFactory = tableFileFactory;
    }

    @ShellMethod(key = "create-database", value = "Create a new database")
    public String createDatabase(String name) throws IOException {
        databaseManager.createDatabase(name);
        return "Database created: " + name;
    }

    @ShellMethod(key = "use", value = "Connect to a database")
    public String useDatabase(String name) {
        databaseManager.useDatabase(name);
        return "Connected to database: " + name;
    }

    @ShellMethod(key = "create-table", value = "Create a table with columns")
    public String createTable(String tableName, String... columnDefinitions) throws IOException {

        // Check if database is selected
        if (databaseManager.getCurrentDatabase() == null) {
            return "Error: No database selected. Use 'use <database-name>' first";
        }

        if (columnDefinitions == null || columnDefinitions.length == 0) {
            return "Error: Please provide at least one column definition (format: columnName:TYPE)";
        }

        // Parse column definitions
        List<ColumnDef> columns = new java.util.ArrayList<>();

        for (String colDef : columnDefinitions) {
            String[] parts = colDef.split(":");
            if (parts.length != 2) {
                return "Error: Invalid column definition '" + colDef + "'. Use format: columnName:TYPE";
            }

            String columnName = parts[0].trim();
            String typeStr = parts[1].trim().toUpperCase();

            try {
                ColumnType type = ColumnType.valueOf(typeStr);
                columns.add(new ColumnDef(columnName, type));
            } catch (IllegalArgumentException e) {
                return "Error: Invalid column type '" + typeStr + "'. Valid types: INT, STRING, DOUBLE, BOOLEAN";
            }
        }

        // Create schema (first column is primary key)
        TableSchema schema = new TableSchema(tableName, columns, 0);

        // Get table path in current database
        Path tablePath = databaseManager.getDatabasePath()
                .resolve(schema.getTableName() + ".tbl");

        // Create table using factory
        tableFileFactory.createTable(schema, tablePath);

        return String.format("Table '%s' created in database '%s' with %d columns\nPrimary key: %s",
                tableName, databaseManager.getCurrentDatabase(), columns.size(), columns.get(0).getName());
    }

    @ShellMethod(key = "sql", value = "Execute SQL command")
    public String executeSQL(String sql) throws IOException {

        // Check if database is selected
        if (databaseManager.getCurrentDatabase() == null) {
            return "Error: No database selected. Use 'use <database-name>' first";
        }

        // Extract table name from SQL
        String tableName = extractTableName(sql);
        if (tableName == null) {
            return "Error: Could not determine table name from SQL";
        }

        // Get table from factory
        try {
            TableFile tableFile = tableFileFactory.getTable(tableName);
            TableSchema schema = tableFileFactory.getSchema(tableName);

            // Create SQL interface and execute
            SQLTableInterface sqlInterface = new SQLTableInterface(tableFile, schema);
            sqlInterface.executeSQL(sql);

            return "✓ SQL executed successfully";

        } catch (IllegalArgumentException e) {
            return "Error: Table '" + tableName + "' not found";
        }
    }

    @ShellMethod(key = "insert", value = "Insert data using SQL")
    public String insert(String sql) throws IOException {
        return executeSQL("INSERT " + sql);
    }

    @ShellMethod(key = "select", value = "Select data using SQL")
    public String select(String sql) throws IOException {
        return executeSQL("SELECT " + sql);
    }

    @ShellMethod(key = "update", value = "Update data using SQL")
    public String update(String sql) throws IOException {
        return executeSQL("UPDATE " + sql);
    }

    @ShellMethod(key = "delete", value = "Delete data using SQL")
    public String delete(String sql) throws IOException {
        return executeSQL("DELETE " + sql);
    }

    // Helper method to extract table name from SQL
    private String extractTableName(String sql) {
        sql = sql.trim().toUpperCase();

        if (sql.startsWith("INSERT INTO")) {
            String[] parts = sql.split("\\s+");
            if (parts.length > 2) {
                return parts[2].toLowerCase();
            }
        } else if (sql.startsWith("UPDATE")) {
            String[] parts = sql.split("\\s+");
            if (parts.length > 1) {
                return parts[1].toLowerCase();
            }
        } else if (sql.startsWith("DELETE FROM")) {
            String[] parts = sql.split("\\s+");
            if (parts.length > 2) {
                return parts[2].toLowerCase();
            }
        } else if (sql.startsWith("SELECT")) {
            // Extract table name from SELECT ... FROM tableName
            int fromIndex = sql.indexOf("FROM");
            if (fromIndex != -1) {
                String[] parts = sql.substring(fromIndex).split("\\s+");
                if (parts.length > 1) {
                    return parts[1].toLowerCase();
                }
            }
        }

        return null;
    }
}
