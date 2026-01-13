package com.simple_rdms.storage_engine.disk_manager;

import com.simple_rdms.storage_engine.page.Page;
import com.simple_rdms.storage_engine.schema.TableSchema;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.simple_rdms.utils.Constants.PAGE_SIZE;

/**
 * File per table format. Each table will have a separate file
 */
public class TableFile {

    private final DiskManager diskManager;
    private final TableSchema schema;
    private final Map<Object, Integer> primaryKeyIndex = new HashMap<>();

    public TableFile(TableSchema schema) throws IOException {
        this.schema = schema;
        this.diskManager = new DiskManager("data/" + schema.getTableName() + ".tbl");

        //Load all indexes when the application restarts since we are storing the primary index on memory.
        for (int i = 0; i < diskManager.pageCount(); i++) {
            Page page = diskManager.readPage(i);
            ByteBuffer buffer = page.buffer();
            buffer.position(8);
            for (int r = 0; r < page.getRowCount(); r++) {
                RowLayout row = RowLayout.deserialize(buffer, schema);

                primaryKeyIndex.put(row.getPrimaryKey(), i);
            }
        }
    }

    /**
     * Insert
     * Serialize the row
     */
    public void insert(RowLayout row) throws IOException {
        //Check if primary key already exists.
        Object primaryKey = row.getPrimaryKey();

        if (primaryKeyIndex.containsKey(primaryKey)) {
            throw new RuntimeException("Primary key violation: " + primaryKey);
        }
        byte[] rowsByte = row.serialize();

        int pageCount = diskManager.pageCount();
        Page page;

        /*
        Create a page if non-exists
         */
        if (pageCount == 0) {
            page = new Page();
            writeRowToPage(page, rowsByte); //Write row to page
            diskManager.writePage(0, page); //Write page to disk
            primaryKeyIndex.put(primaryKey, 0); //Adding the key to primary key index
            return;
        }

        //if page exists, append to the last page since heap file are append-oriented
        page = diskManager.readPage(pageCount - 1);

        //if write to page returns false, then the page is full and we create a new one.
        if (!writeRowToPage(page, rowsByte)) {
            page = new Page();
            writeRowToPage(page, rowsByte);
            diskManager.writePage(pageCount, page);
            primaryKeyIndex.put(primaryKey, pageCount);
        } else {
            diskManager.writePage(pageCount - 1, page);
            primaryKeyIndex.put(primaryKey, pageCount - 1);
        }

    }

    //Select
    public List<RowLayout> readAll() throws IOException {
        List<RowLayout> rows = new ArrayList<>();

        for (int page = 0; page < diskManager.pageCount(); page++) {
            Page pageData = diskManager.readPage(page);
            ByteBuffer buffer = pageData.buffer();
            buffer.position(8); //Skip header

            for (int row = 0; row < pageData.getRowCount(); row++) {
                rows.add(RowLayout.deserialize(buffer, schema));
            }
        }

        return rows;
    }

    private boolean writeRowToPage(Page page, byte[] rowsByte) {
        ByteBuffer buffer = page.buffer();

        //Gets free space info by subtracting page offset from page size
        int offset = page.getOffset();
        int remaining = PAGE_SIZE - offset;

        if (rowsByte.length > remaining) return false;

        //Append row and offset
        buffer.position(offset);
        buffer.put(rowsByte);
        page.setOffset(offset + rowsByte.length);
        page.setRowCount(page.getRowCount() + 1);

        return true;
    }

    public void close() throws IOException {
        diskManager.close();
    }
}
