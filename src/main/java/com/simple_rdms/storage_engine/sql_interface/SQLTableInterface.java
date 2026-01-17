package com.simple_rdms.storage_engine.sql_interface;

import com.simple_rdms.storage_engine.disk_manager.TableFile;
import com.simple_rdms.storage_engine.page.RowLayout;
import com.simple_rdms.storage_engine.schema.ColumnDef;
import com.simple_rdms.storage_engine.schema.TableSchema;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SQLTableInterface {

    private final TableFile tableFile;
    private final TableSchema tableSchema;

    public SQLTableInterface(TableFile tableFile, TableSchema tableSchema) {
        this.tableFile = tableFile;
        this.tableSchema = tableSchema;
    }

    public void executeSQL(String sql) throws IOException {
        sql = sql.trim();

        if (sql.toUpperCase().startsWith("INSERT")) {
            executeInsert(sql);
        } else if (sql.toUpperCase().startsWith("UPDATE")) {
            executeUpdate(sql);
        } else if (sql.toUpperCase().startsWith("DELETE")) {
            executeDelete(sql);
        } else if (sql.toUpperCase().startsWith("SELECT")) {
            executeSelect(sql);
        }
    }

    private void executeInsert(String sql) throws IOException {
        // Support: INSERT INTO table VALUES (val1, val2, val3)
        Pattern pattern = Pattern.compile(
                "INSERT INTO\\s+\\w+\\s+VALUES\\s+\\(([^)]+)\\)",
                Pattern.CASE_INSENSITIVE
        );

        Matcher matcher = pattern.matcher(sql);

        if (!matcher.find()) {
            throw new RuntimeException("Invalid INSERT syntax");
        }

        String valuesStr = matcher.group(1);
        String[] values = splitValues(valuesStr);

        if (values.length != tableSchema.getColumns().size()) {
            throw new RuntimeException(
                    String.format("Column/value count mismatch: expected %d, got %d",
                            tableSchema.getColumns().size(), values.length)
            );
        }

        Object[] data = new Object[values.length];

        for (int i = 0; i < values.length; i++) {
            String val = values[i].trim();
            data[i] = parseValueToObject(val);
        }

        RowLayout rowLayout = new RowLayout(tableSchema, data);
        tableFile.insert(rowLayout);
    }

    private void executeSelect(String sql) throws IOException {
        if (!sql.toUpperCase().contains("*")) {
            throw new RuntimeException("Only SELECT * supported");
        }

        // SELECT * FROM users WHERE id = X
        if (sql.toUpperCase().contains("WHERE")) {
            executeSelectById(sql);
            return;
        }

        List<RowLayout> rows = tableFile.readAll();
        for (RowLayout row : rows) {
            System.out.println(row);
        }
    }

    private void executeUpdate(String sql) throws IOException {
        Pattern pattern = Pattern.compile(
                "UPDATE\\s+(\\w+)\\s+SET\\s+(.+)\\s+WHERE\\s+(\\w+)\\s*=\\s*(\\d+)",
                Pattern.CASE_INSENSITIVE
        );

        Matcher matcher = pattern.matcher(sql);
        if (!matcher.matches()) {
            throw new RuntimeException("Invalid UPDATE syntax");
        }

        String setClause = matcher.group(2);
        int pkValue = Integer.parseInt(matcher.group(4));

        // Load existing row
        RowLayout oldRow = tableFile.findByPrimaryKey(pkValue);
        if (oldRow == null) {
            throw new RuntimeException("Row not found for id=" + pkValue);
        }

        // Parse SET assignments
        Map<String, Object> updates = new HashMap<>();
        String[] assignments = setClause.split(",");

        for (String assignment : assignments) {
            String[] kv = assignment.split("=");
            if (kv.length != 2) {
                throw new RuntimeException("Invalid SET clause");
            }

            String column = kv[0].trim();
            String rawValue = kv[1].trim();

            updates.put(column.toLowerCase(), parseValue(column, rawValue));
        }

        // Build FULL updated row
        Object[] updatedData = new Object[tableSchema.getColumns().size()];

        for (int i = 0; i < tableSchema.getColumns().size(); i++) {
            ColumnDef columnDef = tableSchema.getColumns().get(i);
            String colName = columnDef.getName().toLowerCase();

            if (updates.containsKey(colName)) {
                updatedData[i] = updates.get(colName);
            } else {
                updatedData[i] = oldRow.getValues(i);
            }
        }

        int pkIndex = tableSchema.getPrimaryKeyIndex();
        updatedData[pkIndex] = pkValue;

        // Delegate to storage engine
        tableFile.update(pkValue, new RowLayout(tableSchema, updatedData));
    }

    private void executeSelectById(String sql) throws IOException {
        Pattern pattern = Pattern.compile(
                "SELECT\\s+\\*\\s+FROM\\s+(\\w+)\\s+WHERE\\s+(\\w+)\\s*=\\s*(\\d+)",
                Pattern.CASE_INSENSITIVE
        );

        Matcher matcher = pattern.matcher(sql);
        if (!matcher.matches()) {
            throw new RuntimeException("Invalid SELECT syntax");
        }

        String columnName = matcher.group(2).toLowerCase();
        int id = Integer.parseInt(matcher.group(3));

        // Validate primary key column
        ColumnDef pkColumn = tableSchema.getColumns()
                .get(tableSchema.getPrimaryKeyIndex());

        if (!pkColumn.getName().equalsIgnoreCase(columnName)) {
            throw new RuntimeException("Only primary-key lookup is supported");
        }

        RowLayout row = tableFile.findByPrimaryKey(id);
        System.out.println(row);
    }

    private void executeDelete(String sql) throws IOException {
        Pattern pattern = Pattern.compile(
                "DELETE\\s+FROM\\s+\\w+\\s+WHERE\\s+ID\\s*=\\s*(\\d+)",
                Pattern.CASE_INSENSITIVE
        );

        Matcher matcher = pattern.matcher(sql);
        if (!matcher.find()) {
            throw new RuntimeException("Invalid DELETE syntax");
        }

        int id = Integer.parseInt(matcher.group(1));
        tableFile.delete(id);
    }

    /**
     * Split values handling quoted strings properly
     * Handles: 1, 'John', 'Doe', 'john@example.com'
     */
    private String[] splitValues(String valuesStr) {
        List<String> values = new java.util.ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < valuesStr.length(); i++) {
            char c = valuesStr.charAt(i);

            if (c == '\'' || c == '"') {
                inQuotes = !inQuotes;
                current.append(c);
            } else if (c == ',' && !inQuotes) {
                if (current.length() > 0) {
                    values.add(current.toString().trim());
                    current = new StringBuilder();
                }
            } else {
                current.append(c);
            }
        }

        // Add the last value
        if (current.length() > 0) {
            values.add(current.toString().trim());
        }

        return values.toArray(new String[0]);
    }

    /**
     * Parse a string value to Object (handles quotes and numbers)
     */
    private Object parseValueToObject(String val) {
        val = val.trim();

        // Handle quoted strings
        if ((val.startsWith("'") && val.endsWith("'")) ||
                (val.startsWith("\"") && val.endsWith("\""))) {
            return val.substring(1, val.length() - 1);
        }

        // Try to parse as integer
        try {
            return Integer.parseInt(val);
        } catch (NumberFormatException e) {
            // Try to parse as double
            try {
                return Double.parseDouble(val);
            } catch (NumberFormatException e2) {
                // Return as string
                return val;
            }
        }
    }

    /**
     * Parse value based on column definition
     */
    private Object parseValue(String columnName, String rawValue) {
        rawValue = rawValue.trim();

        ColumnDef column = tableSchema.getColumns()
                .stream()
                .filter(c -> c.getName().equalsIgnoreCase(columnName))
                .findFirst()
                .orElseThrow(() -> new RuntimeException("Unknown column: " + columnName));

        // Remove quotes if present
        if ((rawValue.startsWith("'") && rawValue.endsWith("'")) ||
                (rawValue.startsWith("\"") && rawValue.endsWith("\""))) {
            rawValue = rawValue.substring(1, rawValue.length() - 1);
        }

        return switch (column.getType()) {
            case INT -> Integer.parseInt(rawValue);
            case STRING -> rawValue;
            case BOOLEAN -> Boolean.parseBoolean(rawValue);
            case FLOAT -> Float.parseFloat(rawValue);
            case DOUBLE -> Double.parseDouble(rawValue);
        };
    }
}
