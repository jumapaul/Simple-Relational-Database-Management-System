package com.simple_rdms.storage_engine.page;

public class TableStats {
    public final int totalRows;
    public final int activeRows;
    public final int deletedRows;

    public TableStats(int totalRows, int activeRows, int deletedRows) {
        this.totalRows = totalRows;
        this.activeRows = activeRows;
        this.deletedRows = deletedRows;
    }

    @Override
    public String toString() {
        return String.format("Total: %d, Active: %d, Deleted: %d (%.1f%% fragmentation)",
                totalRows, activeRows, deletedRows,
                totalRows > 0 ? (deletedRows * 100.0 / totalRows) : 0);
    }
}