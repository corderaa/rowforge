package com.rowforge.model;

public class Schema {

    private String sql;
    private int rows;
    private String format;

    public Schema() {}

    public Schema(String sql, int rows, String format) {
        this.sql = sql;
        this.rows = rows;
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

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }
}
