package com.simple_rdms.storage_engine.page;

public class RowLocation {
    private final int pageIndex;
    private final int rowIndex;

    public RowLocation(int pageIndex, int rowIndex) {
        this.pageIndex = pageIndex;
        this.rowIndex = rowIndex;
    }

    public int getPageIndex() {
        return pageIndex;
    }

    public int getRowIndex() {
        return rowIndex;
    }
}
