package com.simple_rdms.storage_engine.disk_manager;


import com.simple_rdms.storage_engine.page.Page;
import com.simple_rdms.storage_engine.page.RowLayout;
import com.simple_rdms.storage_engine.page.RowLocation;
import com.simple_rdms.storage_engine.page.TableStats;
import com.simple_rdms.storage_engine.schema.ColumnDef;
import com.simple_rdms.storage_engine.schema.TableSchema;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

public class TableFile {
    private final DiskManager diskManager;
    private final TableSchema schema;
    // Maps primary key -> RowLocation (pageIndex, rowIndex)
    private final Map<Object, RowLocation> primaryKeyIndex = new HashMap<>();
    // Track deleted rows
    private final Set<Object> deletedKeys = new HashSet<>();

    public TableFile(TableSchema schema) throws IOException {
        this.schema = schema;
        this.diskManager = new DiskManager("data/" + schema.getTableName() + ".tbl");

        // Load all indexes and check deletion flags
        for (int pageIndex = 0; pageIndex < diskManager.pageCount(); pageIndex++) {
            Page page = diskManager.readPage(pageIndex);
            ByteBuffer buffer = page.buffer();
            buffer.position(8);

            for (int rowIndex = 0; rowIndex < page.getRowCount(); rowIndex++) {
                // Read deletion flag (1 byte)
                byte deletedFlag = buffer.get();
                RowLayout row = RowLayout.deserialize(buffer, schema);

                Object primaryKey = row.getPrimaryKey();
                // FIXED: Use RowLocation with both pageIndex AND rowIndex
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
            RowLocation location = primaryKeyIndex.get(primaryKey);
            updateRowAtLocation(location, row, false);
            deletedKeys.remove(primaryKey);
            return;
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

        RowLocation location = primaryKeyIndex.get(primaryKey);
        updateRowAtLocation(location, newRow, false);

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
        int position = 8; // Skip page header
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
        buffer.position(8);

        for (int r = 0; r <= location.getRowIndex(); r++) {
            byte deletedFlag = buffer.get();
            RowLayout row = RowLayout.deserialize(buffer, schema);

            if (r == location.getRowIndex()) {
                return row;
            }
        }

        return null;
    }

    /**
     * Read all non-deleted rows from the table
     */
    public List<RowLayout> readAll() throws IOException {
        List<RowLayout> rows = new ArrayList<>();

        for (int pageIndex = 0; pageIndex < diskManager.pageCount(); pageIndex++) {
            Page pageData = diskManager.readPage(pageIndex);
            ByteBuffer buffer = pageData.buffer();
            buffer.position(8); // Skip header

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
     * Compact the table by removing deleted rows and rewriting pages
     * Call this periodically to reclaim space
     */
    public void compact() throws IOException {
        List<RowLayout> activeRows = readAll(); // Gets only non-deleted rows

        // Clear everything
        primaryKeyIndex.clear();
        deletedKeys.clear();
        diskManager.close();

        // Recreate the file
        DiskManager newDiskManager = new DiskManager("data/" + schema.getTableName() + ".tbl");

        try {
            java.lang.reflect.Field field = this.getClass().getDeclaredField("diskManager");
            field.setAccessible(true);
            field.set(this, newDiskManager);
        } catch (Exception e) {
            throw new RuntimeException("Failed to compact file", e);
        }

        // Reinsert all active rows
        Page currentPage = new Page();
        int currentPageIndex = 0;
        int currentRowIndex = 0;

        for (RowLayout row : activeRows) {
            byte[] rowBytes = row.serialize();

            if (!writeRowToPage(currentPage, rowBytes, false)) {
                newDiskManager.writePage(currentPageIndex, currentPage);
                currentPageIndex++;
                currentRowIndex = 0;
                currentPage = new Page();
                writeRowToPage(currentPage, rowBytes, false);
            }

            primaryKeyIndex.put(row.getPrimaryKey(), new RowLocation(currentPageIndex, currentRowIndex));
            currentRowIndex++;
        }

        if (currentPage.getRowCount() > 0) {
            newDiskManager.writePage(currentPageIndex, currentPage);
        }
    }

    /**
     * Update a row at a specific location
     */
    private void updateRowAtLocation(RowLocation location, RowLayout newRow, boolean isDeleted) throws IOException {
        Page page = diskManager.readPage(location.getPageIndex());
        ByteBuffer buffer = page.buffer();

        // Find the position of the row
        int position = 8;
        for (int i = 0; i < location.getRowIndex(); i++) {
            position++; // Skip deletion flag
            position += getRowSizeAtPosition(buffer, position);
        }

        // Update deletion flag
        buffer.put(position, (byte) (isDeleted ? 1 : 0));
        position++;

        // Write the new row data
        byte[] newRowBytes = newRow.serialize();
        buffer.position(position);
        buffer.put(newRowBytes);

        diskManager.writePage(location.getPageIndex(), page);
    }

    /**
     * Calculate the size of a row in bytes at a specific position
     */
    private int getRowSizeAtPosition(ByteBuffer buffer, int startPosition) {
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

    /**
     * Get the number of active (non-deleted) rows
     */
    public int getRowCount() throws IOException {
        return readAll().size();
    }

    /**
     * Get the total number of rows including deleted ones
     */
    public int getTotalRowCount() throws IOException {
        int count = 0;
        for (int i = 0; i < diskManager.pageCount(); i++) {
            Page page = diskManager.readPage(i);
            count += page.getRowCount();
        }
        return count;
    }

    /**
     * Get statistics about deleted rows
     */
    public TableStats getStats() throws IOException {
        int total = getTotalRowCount();
        int active = getRowCount();
        int deleted = total - active;
        return new TableStats(total, active, deleted);
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

