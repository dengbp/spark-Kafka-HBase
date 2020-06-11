package com.yr.router.strategy;

import com.yr.conf.ConfigurationManager;
import com.yr.constant.Constants;
import com.yr.pool.hbase.HbaseConnectionPool;
import com.yr.pool.tool.ConnectionPoolConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author dengbp
 * @ClassName HbaseStrategy
 * @Description TODO
 * @date 2020-03-12 11:21
 */
public class HbaseConnection implements ConnectStrategy {
    private static Logger logger = LoggerFactory.getLogger(HbaseConnection.class);
    public static HbaseConnectionPool pool;

    @Override
    public Class<? extends ConnectStrategy> getType() {
        return HbaseConnection.class;
    }


    @Override
    public synchronized void open() {
        if (pool != null){
            return;
        }
        logger.info("初始化hbase连接配置...");
        ConnectionPoolConfig config = new ConnectionPoolConfig();
        // 配置连接池参数
        config.setMaxTotal(ConfigurationManager
                .getInteger(Constants.HBASE_POOL_MAX_TOTAL));
        config.setMaxIdle(ConfigurationManager
                .getInteger(Constants.HBASE_POOL_MAX_IDLE));
        config.setMaxWaitMillis(ConfigurationManager
                .getInteger(Constants.HBASE_POOL_MAX_WAITMILLIS));
        config.setTestOnBorrow(ConfigurationManager
                .getBoolean(Constants.HBASE_POOL_TESTONBORROW));
        Configuration hbaseConfig = ConfigurationManager.getHBaseConfiguration();
        pool =new HbaseConnectionPool(config, hbaseConfig);
    }

    @Override
    public synchronized Object connection() {
        return pool.getConnection();
    }

    @Override
    public void returnConnect(Object conn) {
        pool.returnConnection((Connection)conn);
    }
}
