package com.yr.vo;

import org.apache.spark.sql.Row;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author dengbp
 * @ClassName Event
 * @Description TODO
 * @date 2020-03-10 14:31
 */
public final class Event {

    private Row row;
    private Map<String,Object> attach = new ConcurrentHashMap<>();

    public Event(Row row) {
        this.row = row;
    }

    public Event(Row row, Map<String, Object> attach) {
        this.row = row;
        this.attach = attach;
    }

    public Row getRow() {
        return row;
    }

    public void setRow(Row row) {
        this.row = row;
    }

    public Map<String, Object> getAttach() {
        return attach;
    }

    public void setAttach(Map<String, Object> attach) {
        this.attach = attach;
    }
}
