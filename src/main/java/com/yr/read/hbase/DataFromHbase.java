package com.yr.read.hbase;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.yr.conf.ConfigurationManager;
import com.yr.constant.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

/**
 * @author dengbp
 * @ClassName DataFromHbase
 * @Description TODO
 * @date 2020-03-09 11:19
 */
public class DataFromHbase {
    public static void main(String[] args) throws IOException {
        SparkSession spark = SparkSession.builder()
                .appName("big-well")
                .master("local[*]")
                .getOrCreate();
        JavaSparkContext context = new JavaSparkContext(spark.sparkContext());
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", ConfigurationManager
                .getProperty(Constants.ZK_METADATA_BROKER_LIST));
        String tableName = "bill.tb_uhome_acct_item";
        Scan scan = new Scan();
        configuration.set(TableInputFormat.INPUT_TABLE, tableName);
        ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
        String ScanToString = Base64.encodeBytes(proto.toByteArray());
        configuration.set(TableInputFormat.SCAN, ScanToString);
        JavaPairRDD<ImmutableBytesWritable, Result> hbaseRDD = context.newAPIHadoopRDD(configuration, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
        hbaseRDD.cache();// 对myRDD进行缓存
        System.out.println("数据总条数：" + hbaseRDD.count());

        JavaRDD<Row> personsRDD = hbaseRDD.map(new Function<Tuple2<ImmutableBytesWritable, Result>, Row>() {
            @Override
            public Row call(Tuple2<ImmutableBytesWritable, Result> tuple) throws Exception {
                // TODO Auto-generated method stub
                System.out.println("====tuple==========" + tuple);
                Result result = tuple._2();
                String rowKey = Bytes.toString(result.getRow());
                String name = Bytes.toString(result.getValue(Bytes.toBytes(Constants.CF_DEFAULT), Bytes.toBytes("name")));
                String sex = Bytes.toString(result.getValue(Bytes.toBytes(Constants.CF_DEFAULT), Bytes.toBytes("sex")));
                String age = Bytes.toString(result.getValue(Bytes.toBytes(Constants.CF_DEFAULT), Bytes.toBytes("age")));
                return RowFactory.create(rowKey,name, sex, Integer.valueOf(age));
            }
        });
        Dataset dataset = createSchema(spark,personsRDD);
        dataset.printSchema();
        dataset.createOrReplaceTempView("Person");
        Dataset<Row> nameDf = spark.sql("select * from Person ");
        nameDf.show();
    }

    private static Dataset createSchema(SparkSession spark,JavaRDD<Row> personsRDD){
        List<StructField> structFields = new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField("id", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("sex", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
        StructType schema = DataTypes.createStructType(structFields);
        Dataset stuDf = spark.createDataFrame(personsRDD, schema);
        return stuDf;
    }
}

