package com.yr.write.hbase;

import com.alibaba.fastjson.JSONObject;
import com.yr.constant.Constants;
import com.yr.po.hbase.SchemaData;
import com.yr.write.Storage;
import com.yr.router.strategy.ConnectStrategy;
import com.yr.router.strategy.HbaseConnection;
import com.yr.router.strategy.Router;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * @author dengbp
 * @ClassName HbaseStorage
 * @Description TODO
 * @date 2020-03-10 10:31
 */
public class HBaseStorage implements Storage {
    private static Logger logger = LoggerFactory.getLogger(HBaseStorage.class);

    private  SchemaData schemaData;

    public HBaseStorage(SchemaData schemaData) {
        this.schemaData = schemaData;
    }

    @Override
    public boolean storage() {
        logger.info("存储信息[{}]", JSONObject.toJSONString(schemaData));
        return this.save(schemaData);
    }

    private boolean save(SchemaData schemaData){
        boolean save = true;
        Connection conn = (Connection) Router.routing(HbaseConnection.class).connection();
        String hBaseTableName = schemaData.getDbName().concat(".").concat(schemaData.getTableName());
        HTableDescriptor table = new HTableDescriptor(
                TableName.valueOf(hBaseTableName));
        table.addFamily(new HColumnDescriptor(Constants.CF_DEFAULT)
                .setCompressionType(Compression.Algorithm.NONE));

        Table tablePut = null;
        Admin admin = null;
        try {
            tablePut = conn.getTable(TableName.valueOf(hBaseTableName));
            admin = conn.getAdmin();
            TableName tName = table.getTableName();
            if (!admin.tableExists(tName)) {
                try {
                    admin.createTable(table);
                    tablePut = conn.getTable(TableName.valueOf(hBaseTableName));
                    // admin.flush(tName);
                    logger.info("创建表成功");
                } catch (Exception e) {
                    e.printStackTrace();
                    logger.error("建表失败： ->{}",hBaseTableName);
                    save = false;
                }
            }
        } catch (IOException e1) {
            e1.printStackTrace();
            logger.error("获取tablePut或Admin失败： ->{}",hBaseTableName);
            save = false;
        }

        try {
            Put put = setDataPut(schemaData.getTableRowKey(), schemaData.getData());
            tablePut.put(put);
            logger.info("写入数据成功");
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("写入数据失败： ->{}",hBaseTableName);
            save = false;
        }

        try {
            admin.close();
            tablePut.close();
        } catch (IOException e) {
            logger.error("关闭tablePut或Admin失败： ->{}",hBaseTableName);
            save = false;
        }finally {
            ConnectStrategy connectStrategy = Router.routing(HbaseConnection.class);
            connectStrategy.returnConnect(conn);
            return save;
        }
    }

    public void setSchemaData(SchemaData schemaData) {
        this.schemaData = schemaData;
    }

    private Put setDataPut(Object tableRowKey,
                           Map<String, Object> tableData) {
        Put put = new Put(Bytes.toBytes(tableRowKey.toString()));
        for (Map.Entry<String, Object> entry : tableData.entrySet()) {
            if (entry.getValue()==null){
                continue;
            }
            put.addColumn(Bytes.toBytes(Constants.CF_DEFAULT),
                    Bytes.toBytes(entry.getKey()),
                    Bytes.toBytes(entry.getValue().toString()));
        }
        return put;
    }

    @Override
    public Class<? extends Storage> getType() {
        return HBaseStorage.class;
    }
}
