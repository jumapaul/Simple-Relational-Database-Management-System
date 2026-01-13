package com.simple_rdms.storage_engine.page;

import java.nio.ByteBuffer;

import static com.simple_rdms.utils.Constants.PAGE_SIZE;

/**
 * Smallest unit of disk I/O
 */
public class Page {
    private static final int HEADER_SIZE = 8;
    private final ByteBuffer buffer;

    public Page() {
        this.buffer = ByteBuffer.allocate(PAGE_SIZE); //Allocates a fixed memory block for the page
        buffer.putInt(0);
        buffer.putInt(HEADER_SIZE);
    }

    public ByteBuffer buffer() {
        return buffer;
    }

    public int getRowCount() {
        return buffer.getInt(0); //0-> get row count
    }

    public void setRowCount(int count) {
        buffer.putInt(0, count);
    }

    //Offset is the starting and ending point in our byte data.
    public int getOffset() {
        return buffer.getInt(4); //4->Get offset
    }

    public void setOffset(int offset) {
        buffer.putInt(4, offset);
    }

    public byte[] data() {
        return buffer.array();
    }
}
