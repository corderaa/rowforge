package com.rowforge.model;

public class Schema {

    private String sql;
    private int rows;
    private int tables;
    private String format;

    public Schema() {}

    public Schema(String sql, int rows, int tables, String format) {
        this.sql = sql;
        this.rows = rows;
        this.tables = tables;
        this.format = format;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public int getRows() {
        return rows;
    }

    public void setRows(int rows) {
        this.rows = rows;
    }

    public int getTables() {
        return tables;
    }

    public void setTables(int tables) {
        this.tables = tables;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }
}
