package com.simple_rdms.storage_engine.disk_manager;

import com.simple_rdms.storage_engine.page.Page;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;

import static com.simple_rdms.utils.Constants.PAGE_SIZE;

/**
 * Responsible for moving pages between memory and disk.
 */
public class DiskManager {

    private final RandomAccessFile file;

    public DiskManager(Path filePath) throws IOException {
        File file = new File(String.valueOf(filePath));

        //Ensure parent directory exists
        File parent = file.getParentFile();

        if (parent != null && !parent.exists()) {
            parent.mkdirs(); // Create a directory for the name we declared.
        }
        this.file = new RandomAccessFile(file, "rw");
    }

    /**
     * Write page into pageId
     * if offset is 3, then we will have 3 * 4096 = 12288
     * Write starting at byte 12288
     */
    public void writePage(int pageId, Page page) throws IOException {
        file.seek((long) pageId * PAGE_SIZE);
        file.write(page.data());
    }

    public Page readPage(int pageId) throws IOException {
        Page page = new Page(); //Locates new in-memory page buffer.
        file.seek((long) pageId * PAGE_SIZE); //Moving counter to the correct offset
        file.readFully(page.data()); //Blocks until page is entirely read to avoid misinformation

        return page;
    }

    /**
     * Takes the byte length divided by page size to get page count.
     */
    public int pageCount() throws IOException {
        return (int) (file.length() / PAGE_SIZE);
    }

    /*
    Flushes file buffer
    closes file handle
     */

    public void close() throws IOException {
        file.close();
    }
}
