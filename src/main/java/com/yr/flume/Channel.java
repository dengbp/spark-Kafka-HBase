package com.yr.flume;

import com.yr.conf.ConfigurationManager;
import com.yr.constant.Constants;
import com.yr.handle.DefaultHandlerManager;
import com.yr.router.CustomForeachWriter;
import com.yr.router.strategy.Router;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author dengbp
 * @ClassName ModuleRunnerBuilder
 * @Description TODO
 * @date 2020-03-14 21:27
 */
public class Channel {
    private static Logger logger = LoggerFactory.getLogger(Channel.class);
    private static final Channel channel = new Channel();
    private  SparkSession spark;
    private Dataset<Row> df;
    private Class aClass;
    private Channel(){
    }

    static {
        SparkSession spark = SparkSession.builder()
                .appName("big-well")
                .master("local[*]")
                .getOrCreate();
        channel.spark = spark;
    }


    public static Channel sourceBuild(Class primaryClass){
        channel.aClass = primaryClass;
        Dataset<Row> df = channel.spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", ConfigurationManager.getProperty(Constants.KAFKA_METADATA_BROKER_LIST))
                .option("subscribe", "topic.bill_01.tb_uhome_acct_item_owes")
                //.option("subscribePattern", "topic.*")
                .load();
        channel.df = df;
        return channel;
    }

    public Channel sink(){
        Dataset<Row> dataset = channel.df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");
        CustomForeachWriter foreachWriter = new CustomForeachWriter(Router.instance);
        new DefaultHandlerManager(foreachWriter);
        StreamingQuery query = dataset.writeStream()
                .foreach(foreachWriter)
                .outputMode("update").start();
        try {
            query.awaitTermination();
        } catch (StreamingQueryException e) {
            e.printStackTrace();
        }
        return this;
    }
}
