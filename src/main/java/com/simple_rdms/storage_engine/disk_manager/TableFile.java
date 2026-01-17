package com.simple_rdms.storage_engine.disk_manager;

import com.simple_rdms.storage_engine.page.Page;
import com.simple_rdms.storage_engine.page.RowLayout;
import com.simple_rdms.storage_engine.page.RowLocation;
import com.simple_rdms.storage_engine.schema.ColumnDef;
import com.simple_rdms.storage_engine.schema.TableSchema;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.*;


public class TableFile {
    public static final int PAGE_HEADER_SIZE = 8;
    private final DiskManager diskManager;
    private final TableSchema schema;
    // Maps primary key -> RowLocation (pageIndex, rowIndex)
    private final Map<Object, RowLocation> primaryKeyIndex = new HashMap<>();
    // Track deleted rows
    private final Set<Object> deletedKeys = new HashSet<>();

    public TableFile(TableSchema schema, Path filePath) throws IOException {
        this.schema = schema;
        this.diskManager = new DiskManager(filePath);

        // Load all indexes and check deletion flags
        for (int pageIndex = 0; pageIndex < diskManager.pageCount(); pageIndex++) {
            Page page = diskManager.readPage(pageIndex);
            ByteBuffer buffer = page.buffer();
            buffer.position(PAGE_HEADER_SIZE);

            for (int rowIndex = 0; rowIndex < page.getRowCount(); rowIndex++) {
                // Read deletion flag (1 byte)
                byte deletedFlag = buffer.get();
                RowLayout row = RowLayout.deserialize(buffer, schema);

                Object primaryKey = row.getPrimaryKey();
                //Use RowLocation with both pageIndex AND rowIndex
                RowLocation location = new RowLocation(pageIndex, rowIndex);
                primaryKeyIndex.put(primaryKey, location);

                if (deletedFlag == 1) {
                    deletedKeys.add(primaryKey);
                }
            }
        }
    }

    /**
     * Insert a new row
     */
    public void insert(RowLayout row) throws IOException {
        Object primaryKey = row.getPrimaryKey();

        // Check if key exists and is not deleted
        if (primaryKeyIndex.containsKey(primaryKey) && !deletedKeys.contains(primaryKey)) {
            throw new RuntimeException("Primary key violation: " + primaryKey);
        }

        // If the key was previously deleted, we can reuse the slot
        if (deletedKeys.contains(primaryKey)) {
//            RowLocation location = primaryKeyIndex.get(primaryKey);
//            updateRowAtLocation(location, row, false);
            deletedKeys.remove(primaryKey);
            primaryKeyIndex.remove(primaryKey); //Remove old location
        }

        byte[] rowsByte = row.serialize();
        int pageCount = diskManager.pageCount();
        Page page;

        if (pageCount == 0) {
            page = new Page();
            writeRowToPage(page, rowsByte, false);
            diskManager.writePage(0, page);
            primaryKeyIndex.put(primaryKey, new RowLocation(0, 0));
            return;
        }

        page = diskManager.readPage(pageCount - 1);
        int currentRowIndex = page.getRowCount();

        if (!writeRowToPage(page, rowsByte, false)) {
            page = new Page();
            writeRowToPage(page, rowsByte, false);
            diskManager.writePage(pageCount, page);
            primaryKeyIndex.put(primaryKey, new RowLocation(pageCount, 0));
        } else {
            diskManager.writePage(pageCount - 1, page);
            primaryKeyIndex.put(primaryKey, new RowLocation(pageCount - 1, currentRowIndex));
        }
    }

    /**
     * Update a row by primary key
     */
    public boolean update(Object primaryKey, RowLayout newRow) throws IOException {
        if (!primaryKeyIndex.containsKey(primaryKey) || deletedKeys.contains(primaryKey)) {
            return false; // Row not found or deleted
        }

        // Verify the new row has the same primary key
        if (!newRow.getPrimaryKey().equals(primaryKey)) {
            throw new RuntimeException("Cannot change primary key value");
        }

        if (!delete(primaryKey)) return false;

        insert(newRow);

        System.out.println("After update - in deletedKeys: " + deletedKeys.contains(primaryKey));
        System.out.println("After update - in primaryKeyIndex: " + primaryKeyIndex.containsKey(primaryKey));
        return true;
    }

    /**
     * Delete a row by primary key (soft delete with tombstone flag)
     */
    public boolean delete(Object primaryKey) throws IOException {
        if (!primaryKeyIndex.containsKey(primaryKey) || deletedKeys.contains(primaryKey)) {
            return false; // Row not found or already deleted
        }

        RowLocation location = primaryKeyIndex.get(primaryKey);

        // Read the page and mark the row as deleted
        Page page = diskManager.readPage(location.getPageIndex());
        ByteBuffer buffer = page.buffer();

        // Calculate the position of the deletion flag for this row
        int position = PAGE_HEADER_SIZE; // Skip page header
        for (int i = 0; i < location.getRowIndex(); i++) {
            position++; // Skip deletion flag
            position += getRowSize(buffer, position);
        }

        // Set deletion flag to 1
        buffer.put(position, (byte) 1);

        // Write the page back
        diskManager.writePage(location.getPageIndex(), page);

        // Mark as deleted in memory
        deletedKeys.add(primaryKey);

        return true;
    }

    /**
     * Find a single row by primary key
     */
    public RowLayout findByPrimaryKey(Object primaryKey) throws IOException {
        if (!primaryKeyIndex.containsKey(primaryKey) || deletedKeys.contains(primaryKey)) {
            return null;
        }

        RowLocation location = primaryKeyIndex.get(primaryKey);
        Page page = diskManager.readPage(location.getPageIndex());
        ByteBuffer buffer = page.buffer();
//        buffer.position(PAGE_HEADER_SIZE);

        int position = PAGE_HEADER_SIZE;
        for (int r = 0; r <= location.getRowIndex(); r++) {
//            byte deletedFlag = buffer.get();
            position++;
            position += getRowSizeAtPosition(buffer, position);
        }
        buffer.position(position);
        byte deleteFlag = buffer.get();

        if (deleteFlag == 1) {
            return null;
        }
        return RowLayout.deserialize(buffer, schema);
    }

    private int getRowSizeAtPosition(ByteBuffer buffer, int startPosition) {
        int originalPosition = buffer.position();
        buffer.position(startPosition);

        int size = 0;
        for (ColumnDef columnDef : schema.getColumns()) {
            switch (columnDef.getType()) {
                case INT -> {
                    buffer.getInt();
                    size += 4;
                }
                case STRING -> {
                    int len = buffer.getInt();
                    buffer.position(buffer.position() + len);
                    size += 4 + len;
                }
                case BOOLEAN -> {
                    buffer.get();
                    size += 1;
                }
                case FLOAT -> {
                    buffer.getFloat();
                    size += 4;
                }
                case DOUBLE -> {
                    buffer.getDouble();
                    size += 8;
                }
            }
        }

        buffer.position(originalPosition);
        return size;
    }

    /**
     * Read all non-deleted rows from the table
     */
    public List<RowLayout> readAll() throws IOException {
        List<RowLayout> rows = new ArrayList<>();

        for (int pageIndex = 0; pageIndex < diskManager.pageCount(); pageIndex++) {
            Page pageData = diskManager.readPage(pageIndex);
            ByteBuffer buffer = pageData.buffer();
            buffer.position(PAGE_HEADER_SIZE); // Skip header

            for (int rowIndex = 0; rowIndex < pageData.getRowCount(); rowIndex++) {
                byte deletedFlag = buffer.get();
                RowLayout row = RowLayout.deserialize(buffer, schema);

                // Only include non-deleted rows
                if (deletedFlag == 0) {
                    rows.add(row);
                }
            }
        }

        return rows;
    }

    /**
     * Write a row to a page with deletion flag
     */
    private boolean writeRowToPage(Page page, byte[] rowsByte, boolean isDeleted) {
        ByteBuffer buffer = page.buffer();
        int offset = page.getOffset();
        int remaining = 4096 - offset;

        // Need space for deletion flag (1 byte) + row data
        if (rowsByte.length + 1 > remaining) {
            return false;
        }

        buffer.position(offset);
        buffer.put((byte) (isDeleted ? 1 : 0)); // Write deletion flag
        buffer.put(rowsByte);
        page.setOffset(offset + rowsByte.length + 1);
        page.setRowCount(page.getRowCount() + 1);

        return true;
    }

    private int getRowSize(ByteBuffer buffer, int startPosition) {
        int originalPosition = buffer.position();
        buffer.position(startPosition);

        int size = 0;

        for (ColumnDef column : schema.getColumns()) {
            switch (column.getType()) {
                case INT -> {
                    buffer.getInt();
                    size += 4;
                }
                case STRING -> {
                    int len = buffer.getInt();
                    buffer.position(buffer.position() + len);
                    size += 4 + len;
                }
                case BOOLEAN -> {
                    buffer.get();
                    size += 1;
                }
                case FLOAT -> {
                    buffer.getFloat();
                    size += 4;
                }
                case DOUBLE -> {
                    buffer.getDouble();
                    size += 8;
                }
            }
        }

        buffer.position(originalPosition);
        return size;
    }

    public void close() throws IOException {
        diskManager.close();
    }

}

