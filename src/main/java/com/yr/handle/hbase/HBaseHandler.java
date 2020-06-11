package com.yr.handle.hbase;

import com.alibaba.fastjson.JSONObject;
import com.yr.common.Register;
import com.yr.constant.Constants;
import com.yr.handle.AbstractHandler;
import com.yr.handle.Handler;
import com.yr.po.hbase.SchemaData;
import com.yr.write.Storage;
import com.yr.write.hbase.HBaseStorage;
import com.yr.utils.DateUtil;
import com.yr.vo.Event;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author dengbp
 * @ClassName HBaseHandler
 * @Description TODO
 * @date 2020-03-10 10:35
 */
public class HBaseHandler extends AbstractHandler {
    private static Logger logger = LoggerFactory.getLogger(HBaseHandler.class);

    public HBaseHandler(Register register) {
        super(register);
    }

    @Override
    public Class<? extends Handler> getType() {
        return HBaseHandler.class;
    }

    @Override
    public void handle(Event event) {
        Row row = event.getRow();
        SchemaData schemaData = this.analyze(row);
        Storage storage = storageManager.storage(HBaseStorage.class);
        ((HBaseStorage) storage).setSchemaData(schemaData);
        storage.storage();
    }

    private SchemaData analyze(Row row){
        GenericRowWithSchema genericRowWithSchema = (GenericRowWithSchema) row;
        String describe = genericRowWithSchema.get(0).toString();
        logger.info("sync-describe[{}]",describe);
        String schemaDataStr = genericRowWithSchema.get(1).toString();
        try {
            JSONObject jsonObject = JSONObject.parseObject(schemaDataStr);
            String dbName = (String) jsonObject.get("database");
            String tableName = (String) jsonObject.get("table");
            Map tableData = (Map) jsonObject.get("data");
            Object tableRowKey = tableData.get(Constants.TB_UHOME_ACCT_ITEM_OWES_PRIMARY_KEY);
            SchemaData schemaData = new SchemaData(dbName,tableName,tableRowKey,tableData);
            schemaData.setOperateType((String) jsonObject.get("type"));
            schemaData.setTimestamp(jsonObject.getInteger("ts"));
            schemaData.setXid(jsonObject.getInteger("xid"));
            schemaData.setLastModify(DateUtil.current_yyyyMMddHHmmss());
            return schemaData;
        }catch (Exception e){
            e.printStackTrace();
            logger.error("解析数据[{}],异常：{}",JSONObject.toJSONString(schemaDataStr),e.getMessage());
            return null;
        }
    }
}
