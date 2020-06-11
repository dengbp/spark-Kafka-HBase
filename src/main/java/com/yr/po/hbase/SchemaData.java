package com.yr.po.hbase;

import lombok.Data;

import java.io.Serializable;
import java.util.Map;

/**
 * @author dengbp
 * @ClassName SchemaData
 * @Description TODO
 * @date 2020-03-10 17:03
 */
@Data
public class SchemaData implements Serializable {


    private String dbName;
    private String tableName;
    private Object tableRowKey;
    private String operateType;
    private Integer timestamp;
    private Integer xid;
    private String lastModify;
    private Map<String, Object> data;

    public SchemaData(){

    }

    public SchemaData(String dbName, String tableName, Object tableRowKey, Map<String, Object> data) {
        this.dbName = dbName;
        this.tableName = tableName;
        this.tableRowKey = tableRowKey;
        this.data = data;
    }
}
