package com.yr.read.mysql;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.*;

/**
 * @author dengbp
 * @ClassName DataFromMysql
 * @Description TODO
 * @date 2020-03-11 10:42
 */
public class DataFromMysql {
    public static org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(DataFromMysql.class);
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("big-well")
                .config("spark.debug.maxToStringFields", "200")
                .master("local[*]")
                .getOrCreate();
        readMySQL(spark);
    }
    private static void readMySQL(SparkSession spark){

        Dataset<Row> tb_uhome_acct_itemdf = spark.read()
                .format("jdbc")
                .option("url", "jdbc:mysql://192.168.1.12:3306/uhome")
                .option("dbtable","bill_01.tb_uhome_acct_item")
                .option("user", "rentApp")
                .option("password", "abcdefg123")
                .load();
        tb_uhome_acct_itemdf.createOrReplaceTempView("TB_UHOME_ACCT_ITEM");

        Dataset<Row> tb_uhome_housedf = spark.read()
                .format("jdbc")
                .option("url", "jdbc:mysql://192.168.1.12:3306/uhome")
                .option("dbtable","uhome.tb_uhome_house")
                .option("user", "rentApp")
                .option("password", "abcdefg123")
                .load();
        tb_uhome_housedf.createOrReplaceTempView("TB_UHOME_HOUSE");

        Dataset<Row> customerdf = spark.read()
                .format("jdbc")
                .option("url", "jdbc:mysql://192.168.1.12:3306/uhome")
                .option("dbtable","uhome.customer")
                .option("user", "rentApp")
                .option("password", "abcdefg123")
                .load();
        customerdf.createOrReplaceTempView("CUSTOMER");

        Dataset<Row> tb_uhome_fee_item_typedf = spark.read()
                .format("jdbc")
                .option("url", "jdbc:mysql://192.168.1.12:3306/uhome")
                .option("dbtable","uhome.tb_uhome_fee_item_type")
                .option("user", "rentApp")
                .option("password", "abcdefg123")
                .load();
        tb_uhome_fee_item_typedf.createOrReplaceTempView("TB_UHOME_FEE_ITEM_TYPE");

        Dataset<Row> profession_typedf = spark.read()
                .format("jdbc")
                .option("url", "jdbc:mysql://192.168.1.12:3306/uhome")
                .option("dbtable","uhome.profession_type")
                .option("user", "rentApp")
                .option("password", "abcdefg123")
                .load();
        profession_typedf.createOrReplaceTempView("profession_type");

        Dataset<Row> tb_uhome_builddf = spark.read()
                .format("jdbc")
                .option("url", "jdbc:mysql://192.168.1.12:3306/uhome")
                .option("dbtable","uhome.tb_uhome_build")
                .option("user", "rentApp")
                .option("password", "abcdefg123")
                .load();
        tb_uhome_builddf.createOrReplaceTempView("tb_uhome_build");

        Dataset<Row> tb_uhome_stagedf = spark.read()
                .format("jdbc")
                .option("url", "jdbc:mysql://192.168.1.12:3306/uhome")
                .option("dbtable","uhome.TB_UHOME_STAGE")
                .option("user", "rentApp")
                .option("password", "abcdefg123")
                .load();
        tb_uhome_stagedf.createOrReplaceTempView("TB_UHOME_STAGE");

        Dataset<Row> tb_uhome_pay_log_detaildf = spark.read()
                .format("jdbc")
                .option("url", "jdbc:mysql://192.168.1.12:3306/uhome")
                .option("dbtable","bill_01.tb_uhome_pay_log_detail")
                .option("user", "rentApp")
                .option("password", "abcdefg123")
                .load();
        tb_uhome_pay_log_detaildf.createOrReplaceTempView("tb_uhome_pay_log_detail");

        Dataset<Row> tb_uhome_pay_logdf = spark.read()
                .format("jdbc")
                .option("url", "jdbc:mysql://192.168.1.12:3306/uhome")
                .option("dbtable","bill_01.tb_uhome_pay_log")
                .option("user", "rentApp")
                .option("password", "abcdefg123")
                .load();
        tb_uhome_pay_logdf.createOrReplaceTempView("tb_uhome_pay_log");


        Dataset<Row> tb_uhome_acct_item_owesdf = spark.read()
                .format("jdbc")
                .option("url", "jdbc:mysql://192.168.1.12:3306/uhome")
                .option("dbtable","bill_01.tb_uhome_acct_item_owes")
                .option("user", "rentApp")
                .option("password", "abcdefg123")
                .load();
        tb_uhome_acct_item_owesdf.createOrReplaceTempView("tb_uhome_acct_item_owes");





        String sql1 = "SELECT" +
                " t.communityId AS communityId," +
                " t.stageId AS stageId," +
                " t.stageName AS stageName," +
                " t.buildId AS buildId," +
                " t.buildName AS buildName," +
                " t.unitId AS unitId," +
                " t.unitName AS unitName," +
                " t.houseId AS houseId," +
                " t.houseName AS houseName," +
                " t.custId AS custId," +
                " t.custName AS custName," +
                " t.houseStatus AS huseStatus," +
                " pt1.type_name AS houseStatusName," +
                " t.houseStatusType AS houseStatusType," +
                " pt2.type_name AS houseStatusTypeName," +
                " t.feeItemTypeId AS feeItemTypeId," +
                " t.feeItemTypeName AS feeItemTypeName," +
                " t.acctHouseCode AS acctHouseCode," +
                " t.billArea AS billArea," +
                " sum( begin_owes_pre_y_fee ) beginOwesPreYFee," +
                " sum( begin_owes_pre_m_fee ) beginOwesPreMFee," +
                " sum( begin_owes_pre_y_fee ) + sum( begin_owes_pre_m_fee ) beginOwesPreTotalFee," +
                " sum( start_paid_after_fee ) AS startPaidAfterFee," +
                " sum( cur_adjust_pre_fee ) curAdjustPreFee," +
                " sum( cur_adjust_cur_y_pre_fee ) curAdjustCurYPreFee," +
                " sum( cur_adjust_pre_y_pre_fee ) curAdjustPreYPreFee," +
                " sum( cur_sat_receivable_fee ) curStatReceivableFee," +
                " sum( cur_adjust_cur_fee ) curAdjustCurFee," +
                " sum( cur_discount_fee ) curDiscountFee," +
                " sum( cur_add_receivable_fee ) curAddReceivableFee," +
                " sum( cur_total_receivable_fee ) curTotalReceivableFee," +
                " sum( pre_paid_cur_fee ) prePaidCurFee," +
                " sum( cur_paid_pre_y_fee ) curPaidPreYFee," +
                " sum( cur_paid_pre_m_fee ) curPaidPreMFee," +
                " sum( cur_paid_cur_fee ) curPaidCurFee," +
                " sum( pre_paid_cur_fee ) + sum( cur_paid_pre_cur_fee ) + sum( cur_add_pre_paid_cur_fee ) curPaidTotalFee," +
                " sum( pre_paid_cur_fee ) + sum( cur_paid_pre_cur_fee ) + sum( cur_add_pre_paid_cur_fee ) - sum( pre_paid_tax_fee ) - sum( cur_paid_pre_tax_fee ) - sum( cur_add_pre_paid_tax_fee ) curNotTaxTotalFee," +
                " sum( cur_paid_after_fee ) curPaidAfterFee," +
                " sum( pre_paid_after_fee ) + sum( cur_paid_after_fee ) endPaidAfterFee," +
                " sum( cur_total_receivable_fee ) - sum( cur_add_pre_paid_cur_fee ) - sum( pre_paid_cur_fee ) - sum( cur_paid_pre_cur_fee ) - sum( end_owes_total_fee ) AS curAddPrePaidPreFee," +
                " sum( cur_add_pre_paid_cur_fee ) curAddPrePaidCurFee," +
                " sum( cur_add_pre_paid_after_fee ) curAddPrePaidAfterFee," +
                " sum( end_owes_pre_y_fee ) endOwesPreYFee," +
                " sum( end_owes_pre_m_fee ) endOwesPreMFee," +
                " sum( end_owes_cur_fee ) endOwesCurFee," +
                " sum( end_owes_total_fee ) endOwesTotalFee," +
                " cast( SUM( cur_paid_pre_y_fee ) * 1.0 / SUM( begin_owes_pre_y_fee ) AS DECIMAL ( 8, 4 ) ) AS preYearOwesRatio," +
                " cast( SUM( cur_paid_pre_m_fee ) * 1.0 / SUM( begin_owes_pre_m_fee ) AS DECIMAL ( 8, 4 ) ) AS preMonthOwesRatio," +
                " cast( ( SUM( pre_paid_cur_fee ) + SUM( cur_paid_cur_fee ) ) * 1.0 / SUM( cur_sat_receivable_fee ) AS DECIMAL ( 8, 4 ) ) AS satPaidRatio," +
                " cast( ( SUM( pre_paid_cur_fee ) + SUM( cur_paid_cur_fee ) ) * 1.0 / SUM( cur_add_receivable_fee ) AS DECIMAL ( 8, 4 ) ) AS paidRatio," +
                " cast( ( SUM( cur_paid_pre_cur_fee ) ) * 1.0 / SUM( cur_total_receivable_fee ) AS DECIMAL ( 8, 4 ) ) AS totalPaidRatio," +
                " cast( ( SUM( cur_paid_pre_cur_fee ) + SUM( cur_paid_after_fee ) ) * 1.0 / SUM( cur_total_receivable_fee ) AS DECIMAL ( 8, 4 ) ) AS allPaidRatio," +
                " sum( begin_owes_pre_y_fee ) + sum( begin_owes_pre_m_fee ) - ( sum( cur_total_receivable_fee ) - sum( cur_add_pre_paid_cur_fee ) - sum( pre_paid_cur_fee ) - sum( cur_paid_pre_cur_fee ) - sum( end_owes_total_fee ) ) AS beginAccountFee," +
                " sum( start_paid_after_fee ) + sum( cur_add_pre_paid_cur_fee ) + sum( cur_add_pre_paid_after_fee ) AS beginPaidFee," +
                " sum( cur_adjust_cur_fee ) + sum( cur_adjust_pre_fee ) + sum( cur_discount_fee ) AS curAdjustFee," +
                " sum( cur_sat_receivable_fee ) + sum( cur_adjust_cur_fee ) + sum( cur_adjust_pre_fee ) + sum( cur_discount_fee ) AS curResponsibilityFee," +
                " sum( pre_paid_cur_fee ) + sum( cur_paid_pre_cur_fee ) + sum( cur_add_pre_paid_cur_fee ) + sum( cur_paid_after_fee ) AS curActualFee," +
                " sum( begin_owes_pre_y_fee ) + sum( begin_owes_pre_m_fee ) - ( sum( cur_total_receivable_fee ) - sum( cur_add_pre_paid_cur_fee ) - sum( pre_paid_cur_fee ) - sum( cur_paid_pre_cur_fee ) - sum( end_owes_total_fee ) ) - ( sum( start_paid_after_fee ) + sum( cur_add_pre_paid_cur_fee ) + sum( cur_add_pre_paid_after_fee ) ) + ( sum( cur_sat_receivable_fee ) + sum( cur_adjust_cur_fee ) + sum( cur_adjust_pre_fee ) + sum( cur_discount_fee ) ) - ( sum( pre_paid_cur_fee ) + sum( cur_paid_pre_cur_fee ) + sum( cur_add_pre_paid_cur_fee ) + sum( cur_paid_after_fee ) ) - sum( end_owes_total_fee ) + sum( pre_paid_after_fee ) + sum( cur_paid_after_fee ) AS curAuthDutyFee " +
                "FROM" +
                " (" +
                " SELECT" +
                "  A.COMMUNITY_ID AS communityId," +
                "  S.STAGE_ID AS stageId," +
                "  A.HOUSE_STATUS AS houseStatus," +
                "  A.HOUSE_STATUS_TYPE AS houseStatusType," +
                "  S.STAGE_NAME AS stageName," +
                "  D.BUILD_ID AS buildId," +
                "  D.NAME AS buildName," +
                "  C.UNIT_ID AS unitId," +
                "  C.UNIT AS unitName," +
                "  C.HOUSE_ID AS houseId," +
                "  C.HOUSE_NAME AS houseName," +
                "  A.PAY_USERID AS custId," +
                "  IFNULL( E.CUST_NAME, '业主' ) AS custName," +
                "  A.FEE_ITEM_TYPE_ID AS feeItemTypeId," +
                "  F.FEE_ITEM_TYPE_NAME AS feeItemTypeName," +
                "  A.ACCT_HOUSE_CODE AS acctHouseCode," +
                "  A.BILL_AREA AS billArea," +
                "  0 begin_owes_pre_y_fee," +
                "  0 begin_owes_pre_m_fee," +
                "  0 cur_adjust_pre_fee," +
                "  0 cur_adjust_cur_y_pre_fee," +
                "  0 cur_adjust_pre_y_pre_fee," +
                "  sum(" +
                "  IF" +
                "   (" +
                "    A.BILL_FLAG = 3 " +
                "    OR A.task_type IN ( 20, 24, 26, 42, 60, 61, 62, 63 )," +
                "    0," +
                "    A.FEE " +
                "   ) " +
                "  ) cur_sat_receivable_fee," +
                "  sum( IF ( A.task_type = 20, A.FEE, 0 ) ) cur_adjust_cur_fee," +
                "  sum(" +
                "  IF" +
                "   (" +
                "    ( A.bill_Flag = 3 AND A.task_type != 20 ) " +
                "    OR A.task_type IN ( 24, 26, 42, 60, 61, 62, 63 )," +
                "    A.FEE," +
                "    0 " +
                "   ) " +
                "  ) cur_discount_fee," +
                "  sum( A.FEE ) cur_add_receivable_fee," +
                "  sum( A.FEE ) cur_total_receivable_fee," +
                "  0 pre_paid_cur_fee," +
                "  0 pre_paid_tax_fee," +
                "  0 pre_paid_after_fee," +
                "  0 start_paid_after_fee," +
                "  0 cur_paid_pre_y_fee," +
                "  0 cur_paid_pre_m_fee," +
                "  0 cur_paid_cur_fee," +
                "  0 cur_paid_pre_cur_fee," +
                "  0 cur_paid_pre_tax_fee," +
                "  0 cur_paid_after_fee," +
                "  0 cur_add_pre_paid_pre_fee," +
                "  0 cur_add_pre_paid_cur_fee," +
                "  0 cur_add_pre_paid_tax_fee," +
                "  0 cur_add_pre_paid_after_fee," +
                "  0 end_owes_pre_y_fee," +
                "  0 end_owes_pre_m_fee," +
                "  0 end_owes_cur_fee," +
                "  0 end_owes_total_fee " +
                " FROM" +
                "  tb_uhome_acct_item A" +
                "  LEFT JOIN TB_UHOME_HOUSE C ON A.HOUSE_ID = C.HOUSE_ID" +
                "  LEFT JOIN TB_UHOME_BUILD D ON C.BUILD_ID = D.BUILD_ID" +
                "  LEFT JOIN TB_UHOME_STAGE S ON S.STAGE_ID = D.STAGE_ID" +
                "  LEFT JOIN CUSTOMER E ON E.CUST_ID = A.PAY_USERID" +
                "  LEFT JOIN TB_UHOME_FEE_ITEM_TYPE F ON F.FEE_ITEM_TYPE_ID = A.FEE_ITEM_TYPE_ID " +
                " WHERE" +
                "  A.COMMUNITY_ID = 67 " +
                "  AND A.PAY_LIMIT_ID < 30 " +
                "  AND A.BILLING_CYCLE BETWEEN 201901 " +
                "  AND 201912 " +
                " GROUP BY" +
                "  communityId," +
                "  stageId," +
                "  stageName," +
                "  buildId," +
                "  buildName," +
                " unitId," +
                "  unitName," +
               "  houseId," +
                "  houseName," +
                "  custId," +
                " custName," +
                " feeItemTypeName," +
                "  feeItemTypeId," +
                "  billArea," +
                "  acctHouseCode," +
                "  houseStatusType," +
                "  houseStatus UNION ALL" +
                " SELECT" +
                "  A.COMMUNITY_ID AS communityId," +
                "  S.STAGE_ID AS stageId," +
                "  A.HOUSE_STATUS AS houseStatus," +
                "  A.HOUSE_STATUS_TYPE AS houseStatusType," +
                "  S.STAGE_NAME AS stageName," +
                "  D.BUILD_ID AS buildId," +
                "  D.NAME AS buildName," +
                "  C.UNIT_ID AS unitId," +
                "  C.UNIT AS unitName," +
                "  C.HOUSE_ID AS houseId," +
                "  C.HOUSE_NAME AS houseName," +
                "  A.PAY_USERID AS custId," +
                "  IFNULL( E.CUST_NAME, '业主' ) AS custName," +
                "  A.FEE_ITEM_TYPE_ID AS feeItemTypeId," +
                "  F.FEE_ITEM_TYPE_NAME AS feeItemTypeName," +
                "  A.ACCT_HOUSE_CODE AS acctHouseCode," +
                "  A.BILL_AREA AS billArea," +
                "  0 begin_owes_pre_y_fee," +
                "  0 begin_owes_pre_m_fee," +
                "  sum( A.FEE ) cur_adjust_pre_fee," +
                "  sum( IF ( a.BILLING_CYCLE >= 201901 AND a.BILLING_CYCLE < 201901, a.fee, 0 ) ) cur_adjust_cur_y_pre_fee," +
                "  sum( IF ( a.BILLING_CYCLE < 201901, a.fee, 0 ) ) cur_adjust_pre_y_pre_fee," +
                "  0 cur_sat_receivable_fee," +
                "  0 cur_adjust_cur_fee," +
                "  0 cur_discount_fee," +
                "  0 cur_add_receivable_fee," +
                "  sum( A.FEE ) cur_total_receivable_fee," +
                "  0 pre_paid_cur_fee," +
                "  0 pre_paid_tax_fee," +
                "  0 pre_paid_after_fee," +
                "  0 start_paid_after_fee," +
                "  0 cur_paid_pre_y_fee," +
                "  0 cur_paid_pre_m_fee," +
                "  0 cur_paid_cur_fee," +
                "  0 cur_paid_pre_cur_fee," +
                "  0 cur_paid_pre_tax_fee," +
                "  0 cur_paid_after_fee," +
                "  0 cur_add_pre_paid_pre_fee," +
                "  0 cur_add_pre_paid_cur_fee," +
                "  0 cur_add_pre_paid_tax_fee," +
                "  0 cur_add_pre_paid_after_fee," +
                "  0 end_owes_pre_y_fee," +
                "  0 end_owes_pre_m_fee," +
                "  0 end_owes_cur_fee," +
                "  0 end_owes_total_fee " +
                " FROM" +
                "  tb_uhome_acct_item A" +
                "  LEFT JOIN TB_UHOME_HOUSE C ON A.HOUSE_ID = C.HOUSE_ID" +
                "  LEFT JOIN TB_UHOME_BUILD D ON C.BUILD_ID = D.BUILD_ID" +
                "  LEFT JOIN TB_UHOME_STAGE S ON S.STAGE_ID = D.STAGE_ID" +
                "  LEFT JOIN CUSTOMER E ON E.CUST_ID = A.PAY_USERID" +
                "  LEFT JOIN TB_UHOME_FEE_ITEM_TYPE F ON F.FEE_ITEM_TYPE_ID = A.FEE_ITEM_TYPE_ID " +
                " WHERE" +
                "  A.COMMUNITY_ID = 67 " +
                "  AND A.PAY_LIMIT_ID < 30 " +
                "  AND A.BILLING_CYCLE < 201901 " +
                "  AND A.ACCOUNT_CYCLE BETWEEN 201901 " +
                "  AND 201912 " +
                " GROUP BY" +
                "  communityId," +
                "  stageId," +
                "  stageName," +
                "  buildId," +
                "  buildName," +
                " unitId," +
                "  unitName," +
                "  houseId," +
                "  houseName," +
                "  custId," +
                " custName," +
                " feeItemTypeName," +
                "  feeItemTypeId," +
                "  billArea," +
                "  acctHouseCode," +
                "  houseStatusType," +
                "  houseStatus UNION ALL" +
                " SELECT" +
                "  A.COMMUNITY_ID AS communityId," +
                "  S.STAGE_ID AS stageId," +
                "  B.HOUSE_STATUS AS houseStatus," +
                "  B.HOUSE_STATUS_TYPE AS houseStatusType," +
                "  S.STAGE_NAME AS stageName," +
                "  D.BUILD_ID AS buildId," +
                "  D.NAME AS buildName," +
                "  C.UNIT_ID AS unitId," +
                "  C.UNIT AS unitName," +
                "  C.HOUSE_ID AS houseId," +
                "  C.HOUSE_NAME AS houseName," +
                "  B.CUST_ID AS custId," +
                "  IFNULL( E.CUST_NAME, '业主' ) AS custName," +
                "  B.FEE_ITEM_TYPE_ID AS feeItemTypeId," +
                "  F.FEE_ITEM_TYPE_NAME AS feeItemTypeName," +
                "  B.ACCT_HOUSE_CODE AS acctHouseCode," +
                "  B.BILL_AREA AS billArea," +
                "  0 begin_owes_pre_y_fee," +
                "  0 begin_owes_pre_m_fee," +
                "  0 cur_adjust_pre_fee," +
                "  0 cur_adjust_cur_y_pre_fee," +
                "  0 cur_adjust_pre_y_pre_fee," +
                "  0 cur_sat_receivable_fee," +
                "  0 cur_adjust_cur_fee," +
                "  0 cur_discount_fee," +
                "  0 cur_add_receivable_fee," +
                "  0 cur_total_receivable_fee," +
                "  0 pre_paid_cur_fee," +
                "  0 pre_paid_tax_fee," +
                "  0 pre_paid_after_fee," +
                "  0 start_paid_after_fee," +
                "  SUM( IF ( b.billing_cycle < 201901 AND a.pay_cycle BETWEEN 201901 AND 201912, b.fee, 0 ) ) cur_paid_pre_y_fee," +
                "  SUM( IF ( b.billing_cycle >= 201901 AND b.billing_cycle < 201901 AND a.pay_cycle BETWEEN 201901 AND 201912, b.fee, 0 ) ) cur_paid_pre_m_fee," +
                "  SUM( IF ( b.billing_cycle BETWEEN 201901 AND 201912 AND a.pay_cycle BETWEEN 201901 AND 201912, b.fee, 0 ) ) cur_paid_cur_fee," +
                "  SUM( IF ( b.billing_cycle <= 201912 AND a.pay_cycle BETWEEN 201901 AND 201912, b.fee, 0 ) ) cur_paid_pre_cur_fee," +
                "  SUM( IF ( b.billing_cycle <= 201912 AND a.pay_cycle BETWEEN 201901 AND 201912, b.tax_fee, 0 ) ) cur_paid_pre_tax_fee," +
                "  SUM( IF ( b.billing_cycle > 201912 AND a.pay_cycle BETWEEN 201901 AND 201912, b.fee, 0 ) ) cur_paid_after_fee," +
                "  0 cur_add_pre_paid_pre_fee," +
                "  0 cur_add_pre_paid_cur_fee," +
                "  0 cur_add_pre_paid_tax_fee," +
                "  0 cur_add_pre_paid_after_fee," +
                "  SUM( IF ( b.billing_cycle < 201901 AND a.pay_cycle > 201912, b.fee, 0 ) ) end_owes_pre_y_fee," +
                "  SUM( IF ( b.billing_cycle >= 201901 AND b.billing_cycle < 201901 AND a.pay_cycle > 201912, b.fee, 0 ) ) end_owes_pre_m_fee," +
                "  SUM( IF ( b.billing_cycle BETWEEN 201901 AND 201912 AND a.pay_cycle > 201912, b.fee, 0 ) ) end_owes_cur_fee," +
                "  SUM( IF ( b.billing_cycle <= 201912 AND a.pay_cycle > 201912, b.fee, 0 ) ) end_owes_total_fee " +
                " FROM" +
                "  tb_uhome_pay_log A" +
                "  INNER JOIN tb_uhome_pay_log_detail B ON A.PAY_SERIAL_NBR = B.PAY_SERIAL_NBR" +
                "  LEFT JOIN TB_UHOME_HOUSE C ON B.HOUSE_ID = C.HOUSE_ID" +
                "  LEFT JOIN TB_UHOME_BUILD D ON C.BUILD_ID = D.BUILD_ID" +
                "  LEFT JOIN TB_UHOME_STAGE S ON S.STAGE_ID = D.STAGE_ID" +
                "  LEFT JOIN CUSTOMER E ON E.CUST_ID = B.CUST_ID" +
                "  LEFT JOIN TB_UHOME_FEE_ITEM_TYPE F ON F.FEE_ITEM_TYPE_ID = B.FEE_ITEM_TYPE_ID " +
                " WHERE" +
                "  A.COMMUNITY_ID = 67 " +
                "  AND A.PAY_CYCLE >= 201901 " +
                "  AND B.FEE_ITEM_TYPE_ID != 999 " +
                " GROUP BY" +
                "  communityId," +
                "  stageId," +
                "  stageName," +
                "  buildId," +
                "  buildName," +
                " unitId," +
                "  unitName," +
                "  houseId," +
                "  houseName," +
                "  custId," +
                " custName," +
                " feeItemTypeName," +
                "  feeItemTypeId," +
                "  billArea," +
                "  acctHouseCode," +
                "  houseStatusType," +
                "  houseStatus UNION ALL" +
                " SELECT" +
                "  A.COMMUNITY_ID AS communityId," +
                "  S.STAGE_ID AS stageId," +
                "  B.HOUSE_STATUS AS houseStatus," +
                "  B.HOUSE_STATUS_TYPE AS houseStatusType," +
                "  S.STAGE_NAME AS stageName," +
                "  D.BUILD_ID AS buildId," +
                "  D.NAME AS buildName," +
                "  C.UNIT_ID AS unitId," +
                "  C.UNIT AS unitName," +
                "  C.HOUSE_ID AS houseId," +
                "  C.HOUSE_NAME AS houseName," +
                "  B.CUST_ID AS custId," +
                "  IFNULL( E.CUST_NAME, '业主' ) AS custName," +
                "  B.FEE_ITEM_TYPE_ID AS feeItemTypeId," +
                "  F.FEE_ITEM_TYPE_NAME AS feeItemTypeName," +
                "  B.ACCT_HOUSE_CODE AS acctHouseCode," +
                "  B.BILL_AREA AS billArea," +
                "  0 begin_owes_pre_y_fee," +
                "  0 begin_owes_pre_m_fee," +
                "  0 cur_adjust_pre_fee," +
                "  0 cur_adjust_cur_y_pre_fee," +
                "  0 cur_adjust_pre_y_pre_fee," +
                "  0 cur_sat_receivable_fee," +
                "  0 cur_adjust_cur_fee," +
                "  0 cur_discount_fee," +
                "  0 cur_add_receivable_fee," +
                "  0 cur_total_receivable_fee," +
                "  SUM( IF ( b.billing_cycle BETWEEN 201901 AND 201912 AND A.ACCOUNT_CYCLE < 201901, b.fee, 0 ) ) pre_paid_cur_fee," +
                "  SUM( IF ( b.billing_cycle BETWEEN 201901 AND 201912 AND A.ACCOUNT_CYCLE < 201901, b.tax_fee, 0 ) ) pre_paid_tax_fee," +
                "  SUM( IF ( b.billing_cycle > 201912, b.fee, 0 ) ) pre_paid_after_fee," +
                "  SUM( IF ( b.billing_cycle >= 201901 AND A.ACCOUNT_CYCLE < 201901, b.fee, 0 ) ) start_paid_after_fee," +
                "  0 cur_paid_pre_y_fee," +
                "  0 cur_paid_pre_m_fee," +
                "  0 cur_paid_cur_fee," +
                "  0 cur_paid_pre_cur_fee," +
                "  0 cur_paid_pre_tax_fee," +
                "  0 cur_paid_after_fee," +
                "  0 cur_add_pre_paid_pre_fee," +
                "  SUM( IF ( b.billing_cycle BETWEEN 201901 AND 201912 AND A.ACCOUNT_CYCLE BETWEEN 201901 AND 201912, b.fee, 0 ) ) cur_add_pre_paid_cur_fee," +
                "  SUM( IF ( b.billing_cycle BETWEEN 201901 AND 201912 AND A.ACCOUNT_CYCLE BETWEEN 201901 AND 201912, b.tax_fee, 0 ) ) cur_add_pre_paid_tax_fee," +
                "  SUM( IF ( b.billing_cycle > 201912 AND A.ACCOUNT_CYCLE BETWEEN 201901 AND 201912, b.fee, 0 ) ) cur_add_pre_paid_after_fee," +
                "  0 end_owes_pre_y_fee," +
                "  0 end_owes_pre_m_fee," +
                "  0 end_owes_cur_fee," +
                "  0 end_owes_total_fee " +
                " FROM" +
                "  tb_uhome_pay_log A" +
                "  INNER JOIN tb_uhome_pay_log_detail B ON A.PAY_SERIAL_NBR = B.PAY_SERIAL_NBR" +
                "  LEFT JOIN TB_UHOME_HOUSE C ON B.HOUSE_ID = C.HOUSE_ID" +
                "  LEFT JOIN TB_UHOME_BUILD D ON C.BUILD_ID = D.BUILD_ID" +
                "  LEFT JOIN TB_UHOME_STAGE S ON S.STAGE_ID = D.STAGE_ID" +
                "  LEFT JOIN CUSTOMER E ON E.CUST_ID = B.CUST_ID" +
                "  LEFT JOIN TB_UHOME_FEE_ITEM_TYPE F ON F.FEE_ITEM_TYPE_ID = B.FEE_ITEM_TYPE_ID " +
                " WHERE" +
                "  A.COMMUNITY_ID = 67 " +
                "  AND A.PAY_CYCLE < 201901 AND B.BILLING_CYCLE >= 201901 " +
                "  AND B.FEE_ITEM_TYPE_ID != 999 " +
                " GROUP BY" +
                "  communityId," +
                "  stageId," +
                "  stageName," +
                "  buildId," +
                "  buildName," +
                " unitId," +
                "  unitName," +
                "  houseId," +
                "  houseName," +
                "  custId," +
                " custName," +
                " feeItemTypeName," +
                "  feeItemTypeId," +
                "  billArea," +
                "  acctHouseCode," +
                "  houseStatusType," +
                "  houseStatus UNION ALL" +
                " SELECT" +
                "  A.COMMUNITY_ID AS communityId," +
                "  S.STAGE_ID AS stageId," +
                "  B.HOUSE_STATUS AS houseStatus," +
                "  B.HOUSE_STATUS_TYPE AS houseStatusType," +
                "  S.STAGE_NAME AS stageName," +
                "  D.BUILD_ID AS buildId," +
                "  D.NAME AS buildName," +
                "  C.UNIT_ID AS unitId," +
                "  C.UNIT AS unitName," +
                "  C.HOUSE_ID AS houseId," +
                "  C.HOUSE_NAME AS houseName," +
                "  B.CUST_ID AS custId," +
                "  IFNULL( E.CUST_NAME, '业主' ) AS custName," +
                "  B.FEE_ITEM_TYPE_ID AS feeItemTypeId," +
                "  F.FEE_ITEM_TYPE_NAME AS feeItemTypeName," +
                "  B.ACCT_HOUSE_CODE AS acctHouseCode," +
                "  B.BILL_AREA AS billArea," +
                "  SUM( IF ( b.billing_cycle < 201901 AND B.ACCOUNT_CYCLE < 201901, b.fee, 0 ) ) begin_owes_pre_y_fee," +
                "  SUM( IF ( b.billing_cycle >= 201901 AND b.billing_cycle < 201901 AND B.ACCOUNT_CYCLE < 201901, b.fee, 0 ) ) begin_owes_pre_m_fee," +
                "  0 cur_adjust_pre_fee," +
                "  0 cur_adjust_cur_y_pre_fee," +
                "  0 cur_adjust_pre_y_pre_fee," +
                "  0 cur_sat_receivable_fee," +
                "  0 cur_adjust_cur_fee," +
                "  0 cur_discount_fee," +
                "  0 cur_add_receivable_fee," +
                "  SUM( IF ( b.billing_cycle < 201901 AND B.ACCOUNT_CYCLE < 201901, b.fee, 0 ) ) cur_total_receivable_fee," +
                "  0 pre_paid_cur_fee," +
                "  0 pre_paid_tax_fee," +
                "  0 pre_paid_after_fee," +
                "  0 start_paid_after_fee," +
                "  0 cur_paid_pre_y_fee," +
                "  0 cur_paid_pre_m_fee," +
                "  0 cur_paid_cur_fee," +
                "  0 cur_paid_pre_cur_fee," +
                "  0 cur_paid_pre_tax_fee," +
                "  0 cur_paid_after_fee," +
                "  0 cur_add_pre_paid_pre_fee," +
                "  0 cur_add_pre_paid_cur_fee," +
                "  0 cur_add_pre_paid_tax_fee," +
                "  0 cur_add_pre_paid_after_fee," +
                "  0 end_owes_pre_y_fee," +
                "  0 end_owes_pre_m_fee," +
                "  0 end_owes_cur_fee," +
                "  0 end_owes_total_fee " +
                " FROM" +
                "  tb_uhome_pay_log A" +
                "  INNER JOIN tb_uhome_pay_log_detail B ON A.PAY_SERIAL_NBR = B.PAY_SERIAL_NBR" +
                "  LEFT JOIN TB_UHOME_HOUSE C ON B.HOUSE_ID = C.HOUSE_ID" +
                "  LEFT JOIN TB_UHOME_BUILD D ON C.BUILD_ID = D.BUILD_ID" +
                "  LEFT JOIN TB_UHOME_STAGE S ON S.STAGE_ID = D.STAGE_ID" +
                "  LEFT JOIN CUSTOMER E ON E.CUST_ID = B.CUST_ID" +
                "  LEFT JOIN TB_UHOME_FEE_ITEM_TYPE F ON F.FEE_ITEM_TYPE_ID = B.FEE_ITEM_TYPE_ID " +
                " WHERE" +
                "  A.COMMUNITY_ID = 67 " +
                "  AND A.ACCOUNT_CYCLE >= 201901 " +
                "  AND B.FEE_ITEM_TYPE_ID != 999 " +
                " GROUP BY" +
                "  communityId," +
                "  stageId," +
                "  stageName," +
                "  buildId," +
                "  buildName," +
                " unitId," +
                "  unitName," +
                "  houseId," +
                "  houseName," +
                "  custId," +
                " custName," +
                " feeItemTypeName," +
                "  feeItemTypeId," +
                "  billArea," +
                "  acctHouseCode," +
                "  houseStatusType," +
                "  houseStatus UNION ALL" +
                " SELECT" +
                "  A.COMMUNITY_ID AS communityId," +
                "  S.STAGE_ID AS stageId," +
                "  A.HOUSE_STATUS AS houseStatus," +
                "  A.HOUSE_STATUS_TYPE AS houseStatusType," +
                "  S.STAGE_NAME AS stageName," +
                "  D.BUILD_ID AS buildId," +
                "  D.NAME AS buildName," +
                "  C.UNIT_ID AS unitId," +
                "  C.UNIT AS unitName," +
                "  C.HOUSE_ID AS houseId," +
                "  C.HOUSE_NAME AS houseName," +
                "  A.CUST_ID AS custId," +
                "  IFNULL( E.CUST_NAME, '业主' ) AS custName," +
                "  A.FEE_ITEM_TYPE_ID AS feeItemTypeId," +
                "  F.FEE_ITEM_TYPE_NAME AS feeItemTypeName," +
                "  A.ACCT_HOUSE_CODE AS acctHouseCode," +
                "  A.BILL_AREA AS billArea," +
                "  SUM( IF ( a.billing_cycle < 201901 AND A.ACCOUNT_CYCLE < 201901, a.fee, 0 ) ) begin_owes_pre_y_fee," +
                "  SUM( IF ( a.billing_cycle >= 201901 AND a.billing_cycle < 201901 AND A.ACCOUNT_CYCLE < 201901, a.fee, 0 ) ) begin_owes_pre_m_fee," +
                "  0 cur_adjust_pre_fee," +
                "  0 cur_adjust_cur_y_pre_fee," +
                "  0 cur_adjust_pre_y_pre_fee," +
                "  0 cur_sat_receivable_fee," +
                "  0 cur_adjust_cur_fee," +
                "  0 cur_discount_fee," +
                "  0 cur_add_receivable_fee," +
                "  SUM( IF ( a.billing_cycle < 201901 AND A.ACCOUNT_CYCLE < 201901, a.fee, 0 ) ) cur_total_receivable_fee," +
                "  0 pre_paid_cur_fee," +
                "  0 pre_paid_tax_fee," +
                "  0 pre_paid_after_fee," +
                "  0 start_paid_after_fee," +
                "  0 cur_paid_pre_y_fee," +
                "  0 cur_paid_pre_m_fee," +
                "  0 cur_paid_cur_fee," +
                "  0 cur_paid_pre_cur_fee," +
                "  0 cur_paid_pre_tax_fee," +
                "  0 cur_paid_after_fee," +
                "  0 cur_add_pre_paid_pre_fee," +
                "  0 cur_add_pre_paid_cur_fee," +
                "  0 cur_add_pre_paid_tax_fee," +
                "  0 cur_add_pre_paid_after_fee," +
                "  SUM( IF ( a.billing_cycle < 201901, a.fee, 0 ) ) end_owes_pre_y_fee," +
                "  SUM( IF ( a.billing_cycle >= 201901 AND a.billing_cycle < 201901, a.fee, 0 ) ) end_owes_pre_m_fee," +
                "  SUM( IF ( a.billing_cycle BETWEEN 201901 AND 201912, a.fee, 0 ) ) end_owes_cur_fee," +
                "  SUM( a.fee ) end_owes_total_fee " +
                " FROM" +
                "  tb_uhome_acct_item_owes A" +
                "  LEFT JOIN TB_UHOME_HOUSE C ON A.HOUSE_ID = C.HOUSE_ID" +
                "  LEFT JOIN TB_UHOME_BUILD D ON C.BUILD_ID = D.BUILD_ID" +
                "  LEFT JOIN TB_UHOME_STAGE S ON S.STAGE_ID = D.STAGE_ID" +
                "  LEFT JOIN CUSTOMER E ON E.CUST_ID = A.CUST_ID" +
                "  LEFT JOIN TB_UHOME_FEE_ITEM_TYPE F ON F.FEE_ITEM_TYPE_ID = A.FEE_ITEM_TYPE_ID " +
                " WHERE" +
                "  A.COMMUNITY_ID = 67 " +
                "  AND A.BILLING_CYCLE <= 201912 " +
                "  AND A.PAY_LIMIT_ID < 30 " +
                " GROUP BY" +
                "  communityId," +
                "  stageId," +
                "  stageName," +
                "  buildId," +
                "  buildName," +
                " unitId," +
                "  unitName," +
                "  houseId," +
                "  houseName," +
                "  custId," +
                " custName," +
                " feeItemTypeName," +
                "  feeItemTypeId," +
                "  billArea," +
                "  acctHouseCode," +
                "  houseStatusType," +
                "  houseStatus " +
                " ) t" +
                " LEFT JOIN profession_type pt1 ON pt1.category_code = 50 " +
                " AND pt1.type_code = t.houseStatus" +
                " LEFT JOIN profession_type pt2 ON pt2.category_code = 40 " +
                " AND pt2.type_code = t.houseStatusType " +
                "GROUP BY" +
                " communityId," +
                " stageId," +
                "  stageName," +
                "  buildId," +
                "  buildId," +
                " buildName," +
                " unitId," +
                " unitName," +
                " houseId," +
                " houseName," +
                " custId," +
                " custName," +
                " feeItemTypeName," +
                " feeItemTypeId," +
                " billArea," +
                " acctHouseCode," +
                " houseStatusType," +
                " houseStatusTypeName," +
                " houseStatusName," +
                " houseStatus";


        String sql2 = "SELECT " +
                " t.communityId AS communityId, " +
                " t.stageId AS stageId, " +
                " t.stageName AS stageName, " +
                " t.buildId AS buildId, " +
                " t.buildName AS buildName, " +
                " t.unitId AS unitId, " +
                " t.unitName AS unitName, " +
                " t.houseId AS houseId, " +
                " t.houseName AS houseName, " +
                " t.custId AS custId, " +
                " t.custName AS custName, " +
                " t.houseStatus AS huseStatus, " +
                " pt1.type_name AS houseStatusName, " +
                " t.houseStatusType AS houseStatusType, " +
                " pt2.type_name AS houseStatusTypeName, " +
                " t.feeItemTypeId AS feeItemTypeId, " +
                " t.feeItemTypeName AS feeItemTypeName, " +
                " t.acctHouseCode AS acctHouseCode, " +
                " t.billArea AS billArea, " +
                " sum( begin_owes_pre_y_fee ) beginOwesPreYFee, " +
                " sum( begin_owes_pre_m_fee ) beginOwesPreMFee, " +
                " sum( begin_owes_pre_y_fee ) + sum( begin_owes_pre_m_fee ) beginOwesPreTotalFee, " +
                " sum( start_paid_after_fee ) AS startPaidAfterFee, " +
                " sum( cur_adjust_pre_fee ) curAdjustPreFee, " +
                " sum( cur_adjust_cur_y_pre_fee ) curAdjustCurYPreFee, " +
                " sum( cur_adjust_pre_y_pre_fee ) curAdjustPreYPreFee, " +
                " sum( cur_sat_receivable_fee ) curStatReceivableFee, " +
                " sum( cur_adjust_cur_fee ) curAdjustCurFee, " +
                " sum( cur_discount_fee ) curDiscountFee, " +
                " sum( cur_add_receivable_fee ) curAddReceivableFee, " +
                " sum( cur_total_receivable_fee ) curTotalReceivableFee, " +
                " sum( pre_paid_cur_fee ) prePaidCurFee, " +
                " sum( cur_paid_pre_y_fee ) curPaidPreYFee, " +
                " sum( cur_paid_pre_m_fee ) curPaidPreMFee, " +
                " sum( cur_paid_cur_fee ) curPaidCurFee, " +
                " sum( pre_paid_cur_fee ) + sum( cur_paid_pre_cur_fee ) + sum( cur_add_pre_paid_cur_fee ) curPaidTotalFee, " +
                " sum( pre_paid_cur_fee ) + sum( cur_paid_pre_cur_fee ) + sum( cur_add_pre_paid_cur_fee ) - sum( pre_paid_tax_fee ) - sum( cur_paid_pre_tax_fee ) - sum( cur_add_pre_paid_tax_fee ) curNotTaxTotalFee, " +
                " sum( cur_paid_after_fee ) curPaidAfterFee, " +
                " sum( pre_paid_after_fee ) + sum( cur_paid_after_fee ) endPaidAfterFee, " +
                " sum( cur_total_receivable_fee ) - sum( cur_add_pre_paid_cur_fee ) - sum( pre_paid_cur_fee ) - sum( cur_paid_pre_cur_fee ) - sum( end_owes_total_fee ) AS curAddPrePaidPreFee, " +
                " sum( cur_add_pre_paid_cur_fee ) curAddPrePaidCurFee, " +
                " sum( cur_add_pre_paid_after_fee ) curAddPrePaidAfterFee, " +
                " sum( end_owes_pre_y_fee ) endOwesPreYFee, " +
                " sum( end_owes_pre_m_fee ) endOwesPreMFee, " +
                " sum( end_owes_cur_fee ) endOwesCurFee, " +
                " sum( end_owes_total_fee ) endOwesTotalFee, " +
                " cast( SUM( cur_paid_pre_y_fee ) * 1.0 / SUM( begin_owes_pre_y_fee ) AS DECIMAL ( 8, 4 ) ) AS preYearOwesRatio, " +
                " cast( SUM( cur_paid_pre_m_fee ) * 1.0 / SUM( begin_owes_pre_m_fee ) AS DECIMAL ( 8, 4 ) ) AS preMonthOwesRatio, " +
                " cast( ( SUM( pre_paid_cur_fee ) + SUM( cur_paid_cur_fee ) ) * 1.0 / SUM( cur_sat_receivable_fee ) AS DECIMAL ( 8, 4 ) ) AS satPaidRatio, " +
                " cast( ( SUM( pre_paid_cur_fee ) + SUM( cur_paid_cur_fee ) ) * 1.0 / SUM( cur_add_receivable_fee ) AS DECIMAL ( 8, 4 ) ) AS paidRatio, " +
                " cast( ( SUM( cur_paid_pre_cur_fee ) ) * 1.0 / SUM( cur_total_receivable_fee ) AS DECIMAL ( 8, 4 ) ) AS totalPaidRatio, " +
                " cast( ( SUM( cur_paid_pre_cur_fee ) + SUM( cur_paid_after_fee ) ) * 1.0 / SUM( cur_total_receivable_fee ) AS DECIMAL ( 8, 4 ) ) AS allPaidRatio, " +
                " sum( begin_owes_pre_y_fee ) + sum( begin_owes_pre_m_fee ) - ( sum( cur_total_receivable_fee ) - sum( cur_add_pre_paid_cur_fee ) - sum( pre_paid_cur_fee ) - sum( cur_paid_pre_cur_fee ) - sum( end_owes_total_fee ) ) AS beginAccountFee, " +
                " sum( start_paid_after_fee ) + sum( cur_add_pre_paid_cur_fee ) + sum( cur_add_pre_paid_after_fee ) AS beginPaidFee, " +
                " sum( cur_adjust_cur_fee ) + sum( cur_adjust_pre_fee ) + sum( cur_discount_fee ) AS curAdjustFee, " +
                " sum( cur_sat_receivable_fee ) + sum( cur_adjust_cur_fee ) + sum( cur_adjust_pre_fee ) + sum( cur_discount_fee ) AS curResponsibilityFee, " +
                " sum( pre_paid_cur_fee ) + sum( cur_paid_pre_cur_fee ) + sum( cur_add_pre_paid_cur_fee ) + sum( cur_paid_after_fee ) AS curActualFee, " +
                " sum( begin_owes_pre_y_fee ) + sum( begin_owes_pre_m_fee ) - ( sum( cur_total_receivable_fee ) - sum( cur_add_pre_paid_cur_fee ) - sum( pre_paid_cur_fee ) - sum( cur_paid_pre_cur_fee ) - sum( end_owes_total_fee ) ) - ( sum( start_paid_after_fee ) + sum( cur_add_pre_paid_cur_fee ) + sum( cur_add_pre_paid_after_fee ) ) + ( sum( cur_sat_receivable_fee ) + sum( cur_adjust_cur_fee ) + sum( cur_adjust_pre_fee ) + sum( cur_discount_fee ) ) - ( sum( pre_paid_cur_fee ) + sum( cur_paid_pre_cur_fee ) + sum( cur_add_pre_paid_cur_fee ) + sum( cur_paid_after_fee ) ) - sum( end_owes_total_fee ) + sum( pre_paid_after_fee ) + sum( cur_paid_after_fee ) AS curAuthDutyFee  " +
                "FROM " +
                " ( " +
                " SELECT " +
                "  A.COMMUNITY_ID AS communityId, " +
                "  S.STAGE_ID AS stageId, " +
                "  A.HOUSE_STATUS AS houseStatus, " +
                "  A.HOUSE_STATUS_TYPE AS houseStatusType, " +
                "  S.STAGE_NAME AS stageName, " +
                "  D.BUILD_ID AS buildId, " +
                "  D.NAME AS buildName, " +
                "  C.UNIT_ID AS unitId, " +
                "  C.UNIT AS unitName, " +
                "  C.HOUSE_ID AS houseId, " +
                "  C.HOUSE_NAME AS houseName, " +
                "  A.PAY_USERID AS custId, " +
                "  IFNULL( E.CUST_NAME, '业主' ) AS custName, " +
                "  A.FEE_ITEM_TYPE_ID AS feeItemTypeId, " +
                "  F.FEE_ITEM_TYPE_NAME AS feeItemTypeName, " +
                "  A.ACCT_HOUSE_CODE AS acctHouseCode, " +
                "  A.BILL_AREA AS billArea, " +
                "  0 begin_owes_pre_y_fee, " +
                "  0 begin_owes_pre_m_fee, " +
                "  0 cur_adjust_pre_fee, " +
                "  0 cur_adjust_cur_y_pre_fee, " +
                "  0 cur_adjust_pre_y_pre_fee, " +
                "  sum( " +
                "  IF " +
                "   ( " +
                "    A.BILL_FLAG = 3  " +
                "    OR A.task_type IN ( 20, 24, 26, 42, 60, 61, 62, 63 ), " +
                "    0, " +
                "    A.FEE  " +
                "   )  " +
                "  ) cur_sat_receivable_fee, " +
                "  sum( IF ( A.task_type = 20, A.FEE, 0 ) ) cur_adjust_cur_fee, " +
                "  sum( " +
                "  IF " +
                "   ( " +
                "    ( A.bill_Flag = 3 AND A.task_type != 20 )  " +
                "    OR A.task_type IN ( 24, 26, 42, 60, 61, 62, 63 ), " +
                "    A.FEE, " +
                "    0  " +
                "   )  " +
                "  ) cur_discount_fee, " +
                "  sum( A.FEE ) cur_add_receivable_fee, " +
                "  sum( A.FEE ) cur_total_receivable_fee, " +
                "  0 pre_paid_cur_fee, " +
                "  0 pre_paid_tax_fee, " +
                "  0 pre_paid_after_fee, " +
                "  0 start_paid_after_fee, " +
                "  0 cur_paid_pre_y_fee, " +
                "  0 cur_paid_pre_m_fee, " +
                "  0 cur_paid_cur_fee, " +
                "  0 cur_paid_pre_cur_fee, " +
                "  0 cur_paid_pre_tax_fee, " +
                "  0 cur_paid_after_fee, " +
                "  0 cur_add_pre_paid_pre_fee, " +
                "  0 cur_add_pre_paid_cur_fee, " +
                "  0 cur_add_pre_paid_tax_fee, " +
                "  0 cur_add_pre_paid_after_fee, " +
                "  0 end_owes_pre_y_fee, " +
                "  0 end_owes_pre_m_fee, " +
                "  0 end_owes_cur_fee, " +
                "  0 end_owes_total_fee  " +
                " FROM " +
                "   tb_uhome_acct_item A " +
                "  LEFT JOIN TB_UHOME_HOUSE C ON A.HOUSE_ID = C.HOUSE_ID " +
                "  LEFT JOIN TB_UHOME_BUILD D ON C.BUILD_ID = D.BUILD_ID " +
                "  LEFT JOIN TB_UHOME_STAGE S ON S.STAGE_ID = D.STAGE_ID " +
                "  LEFT JOIN CUSTOMER E ON E.CUST_ID = A.PAY_USERID " +
                "  LEFT JOIN TB_UHOME_FEE_ITEM_TYPE F ON F.FEE_ITEM_TYPE_ID = A.FEE_ITEM_TYPE_ID  " +
                " WHERE " +
                "  A.COMMUNITY_ID = 67  " +
                "  AND A.PAY_LIMIT_ID < 30  " +
                "  AND A.BILLING_CYCLE BETWEEN 201901  " +
                "  AND 201912  " +
                " GROUP BY " +
                "  communityId " +
                "  UNION ALL " +
                " SELECT " +
                "  A.COMMUNITY_ID AS communityId, " +
                "  S.STAGE_ID AS stageId, " +
                "  A.HOUSE_STATUS AS houseStatus, " +
                "  A.HOUSE_STATUS_TYPE AS houseStatusType, " +
                "  S.STAGE_NAME AS stageName, " +
                "  D.BUILD_ID AS buildId, " +
                "  D.NAME AS buildName, " +
                "  C.UNIT_ID AS unitId, " +
                "  C.UNIT AS unitName, " +
                "  C.HOUSE_ID AS houseId, " +
                "  C.HOUSE_NAME AS houseName, " +
                "  A.PAY_USERID AS custId, " +
                "  IFNULL( E.CUST_NAME, '业主' ) AS custName, " +
                "  A.FEE_ITEM_TYPE_ID AS feeItemTypeId, " +
                "  F.FEE_ITEM_TYPE_NAME AS feeItemTypeName, " +
                "  A.ACCT_HOUSE_CODE AS acctHouseCode, " +
                "  A.BILL_AREA AS billArea, " +
                "  0 begin_owes_pre_y_fee, " +
                "  0 begin_owes_pre_m_fee, " +
                "  sum( A.FEE ) cur_adjust_pre_fee, " +
                "  sum( IF ( a.BILLING_CYCLE >= 201901 AND a.BILLING_CYCLE < 201901, a.fee, 0 ) ) cur_adjust_cur_y_pre_fee, " +
                "  sum( IF ( a.BILLING_CYCLE < 201901, a.fee, 0 ) ) cur_adjust_pre_y_pre_fee, " +
                "  0 cur_sat_receivable_fee, " +
                "  0 cur_adjust_cur_fee, " +
                "  0 cur_discount_fee, " +
                "  0 cur_add_receivable_fee, " +
                "  sum( A.FEE ) cur_total_receivable_fee, " +
                "  0 pre_paid_cur_fee, " +
                "  0 pre_paid_tax_fee, " +
                "  0 pre_paid_after_fee, " +
                "  0 start_paid_after_fee, " +
                "  0 cur_paid_pre_y_fee, " +
                "  0 cur_paid_pre_m_fee, " +
                "  0 cur_paid_cur_fee, " +
                "  0 cur_paid_pre_cur_fee, " +
                "  0 cur_paid_pre_tax_fee, " +
                "  0 cur_paid_after_fee, " +
                "  0 cur_add_pre_paid_pre_fee, " +
                "  0 cur_add_pre_paid_cur_fee, " +
                "  0 cur_add_pre_paid_tax_fee, " +
                "  0 cur_add_pre_paid_after_fee, " +
                "  0 end_owes_pre_y_fee, " +
                "  0 end_owes_pre_m_fee, " +
                "  0 end_owes_cur_fee, " +
                "  0 end_owes_total_fee  " +
                " FROM " +
                "   tb_uhome_acct_item A " +
                "  LEFT JOIN TB_UHOME_HOUSE C ON A.HOUSE_ID = C.HOUSE_ID " +
                "  LEFT JOIN TB_UHOME_BUILD D ON C.BUILD_ID = D.BUILD_ID " +
                "  LEFT JOIN TB_UHOME_STAGE S ON S.STAGE_ID = D.STAGE_ID " +
                "  LEFT JOIN CUSTOMER E ON E.CUST_ID = A.PAY_USERID " +
                "  LEFT JOIN TB_UHOME_FEE_ITEM_TYPE F ON F.FEE_ITEM_TYPE_ID = A.FEE_ITEM_TYPE_ID  " +
                " WHERE " +
                "  A.COMMUNITY_ID = 67  " +
                "  AND A.PAY_LIMIT_ID < 30  " +
                "  AND A.BILLING_CYCLE < 201901  " +
                "  AND A.ACCOUNT_CYCLE BETWEEN 201901  " +
                "  AND 201912  " +
                " GROUP BY " +
                "  communityId " +
                "  UNION ALL " +
                " SELECT " +
                "  A.COMMUNITY_ID AS communityId, " +
                "  S.STAGE_ID AS stageId, " +
                "  B.HOUSE_STATUS AS houseStatus, " +
                "  B.HOUSE_STATUS_TYPE AS houseStatusType, " +
                "  S.STAGE_NAME AS stageName, " +
                "  D.BUILD_ID AS buildId, " +
                "  D.NAME AS buildName, " +
                "  C.UNIT_ID AS unitId, " +
                "  C.UNIT AS unitName, " +
                "  C.HOUSE_ID AS houseId, " +
                "  C.HOUSE_NAME AS houseName, " +
                "  B.CUST_ID AS custId, " +
                "  IFNULL( E.CUST_NAME, '业主' ) AS custName, " +
                "  B.FEE_ITEM_TYPE_ID AS feeItemTypeId, " +
                "  F.FEE_ITEM_TYPE_NAME AS feeItemTypeName, " +
                "  B.ACCT_HOUSE_CODE AS acctHouseCode, " +
                "  B.BILL_AREA AS billArea, " +
                "  0 begin_owes_pre_y_fee, " +
                "  0 begin_owes_pre_m_fee, " +
                "  0 cur_adjust_pre_fee, " +
                "  0 cur_adjust_cur_y_pre_fee, " +
                "  0 cur_adjust_pre_y_pre_fee, " +
                "  0 cur_sat_receivable_fee, " +
                "  0 cur_adjust_cur_fee, " +
                "  0 cur_discount_fee, " +
                "  0 cur_add_receivable_fee, " +
                "  0 cur_total_receivable_fee, " +
                "  0 pre_paid_cur_fee, " +
                "  0 pre_paid_tax_fee, " +
                "  0 pre_paid_after_fee, " +
                "  0 start_paid_after_fee, " +
                "  SUM( IF ( b.billing_cycle < 201901 AND a.pay_cycle BETWEEN 201901 AND 201912, b.fee, 0 ) ) cur_paid_pre_y_fee, " +
                "  SUM( IF ( b.billing_cycle >= 201901 AND b.billing_cycle < 201901 AND a.pay_cycle BETWEEN 201901 AND 201912, b.fee, 0 ) ) cur_paid_pre_m_fee, " +
                "  SUM( IF ( b.billing_cycle BETWEEN 201901 AND 201912 AND a.pay_cycle BETWEEN 201901 AND 201912, b.fee, 0 ) ) cur_paid_cur_fee, " +
                "  SUM( IF ( b.billing_cycle <= 201912 AND a.pay_cycle BETWEEN 201901 AND 201912, b.fee, 0 ) ) cur_paid_pre_cur_fee, " +
                "  SUM( IF ( b.billing_cycle <= 201912 AND a.pay_cycle BETWEEN 201901 AND 201912, b.tax_fee, 0 ) ) cur_paid_pre_tax_fee, " +
                "  SUM( IF ( b.billing_cycle > 201912 AND a.pay_cycle BETWEEN 201901 AND 201912, b.fee, 0 ) ) cur_paid_after_fee, " +
                "  0 cur_add_pre_paid_pre_fee, " +
                "  0 cur_add_pre_paid_cur_fee, " +
                "  0 cur_add_pre_paid_tax_fee, " +
                "  0 cur_add_pre_paid_after_fee, " +
                "  SUM( IF ( b.billing_cycle < 201901 AND a.pay_cycle > 201912, b.fee, 0 ) ) end_owes_pre_y_fee, " +
                "  SUM( IF ( b.billing_cycle >= 201901 AND b.billing_cycle < 201901 AND a.pay_cycle > 201912, b.fee, 0 ) ) end_owes_pre_m_fee, " +
                "  SUM( IF ( b.billing_cycle BETWEEN 201901 AND 201912 AND a.pay_cycle > 201912, b.fee, 0 ) ) end_owes_cur_fee, " +
                "  SUM( IF ( b.billing_cycle <= 201912 AND a.pay_cycle > 201912, b.fee, 0 ) ) end_owes_total_fee  " +
                " FROM " +
                "   tb_uhome_pay_log A " +
                "  INNER JOIN  tb_uhome_pay_log_detail B ON A.PAY_SERIAL_NBR = B.PAY_SERIAL_NBR " +
                "  LEFT JOIN TB_UHOME_HOUSE C ON B.HOUSE_ID = C.HOUSE_ID " +
                "  LEFT JOIN TB_UHOME_BUILD D ON C.BUILD_ID = D.BUILD_ID " +
                "  LEFT JOIN TB_UHOME_STAGE S ON S.STAGE_ID = D.STAGE_ID " +
                "  LEFT JOIN CUSTOMER E ON E.CUST_ID = B.CUST_ID " +
                "  LEFT JOIN TB_UHOME_FEE_ITEM_TYPE F ON F.FEE_ITEM_TYPE_ID = B.FEE_ITEM_TYPE_ID  " +
                " WHERE " +
                "  A.COMMUNITY_ID = 67  " +
                "  AND A.PAY_CYCLE >= 201901  " +
                "  AND B.FEE_ITEM_TYPE_ID != 999  " +
                " GROUP BY " +
                "  communityId " +
                "  UNION ALL " +
                " SELECT " +
                "  A.COMMUNITY_ID AS communityId, " +
                "  S.STAGE_ID AS stageId, " +
                "  B.HOUSE_STATUS AS houseStatus, " +
                "  B.HOUSE_STATUS_TYPE AS houseStatusType, " +
                "  S.STAGE_NAME AS stageName, " +
                "  D.BUILD_ID AS buildId, " +
                "  D.NAME AS buildName, " +
                "  C.UNIT_ID AS unitId, " +
                "  C.UNIT AS unitName, " +
                "  C.HOUSE_ID AS houseId, " +
                "  C.HOUSE_NAME AS houseName, " +
                "  B.CUST_ID AS custId, " +
                "  IFNULL( E.CUST_NAME, '业主' ) AS custName, " +
                "  B.FEE_ITEM_TYPE_ID AS feeItemTypeId, " +
                "  F.FEE_ITEM_TYPE_NAME AS feeItemTypeName, " +
                "  B.ACCT_HOUSE_CODE AS acctHouseCode, " +
                "  B.BILL_AREA AS billArea, " +
                "  0 begin_owes_pre_y_fee, " +
                "  0 begin_owes_pre_m_fee, " +
                "  0 cur_adjust_pre_fee, " +
                "  0 cur_adjust_cur_y_pre_fee, " +
                "  0 cur_adjust_pre_y_pre_fee, " +
                "  0 cur_sat_receivable_fee, " +
                "  0 cur_adjust_cur_fee, " +
                "  0 cur_discount_fee, " +
                "  0 cur_add_receivable_fee, " +
                "  0 cur_total_receivable_fee, " +
                "  SUM( IF ( b.billing_cycle BETWEEN 201901 AND 201912 AND A.ACCOUNT_CYCLE < 201901, b.fee, 0 ) ) pre_paid_cur_fee, " +
                "  SUM( IF ( b.billing_cycle BETWEEN 201901 AND 201912 AND A.ACCOUNT_CYCLE < 201901, b.tax_fee, 0 ) ) pre_paid_tax_fee, " +
                "  SUM( IF ( b.billing_cycle > 201912, b.fee, 0 ) ) pre_paid_after_fee, " +
                "  SUM( IF ( b.billing_cycle >= 201901 AND A.ACCOUNT_CYCLE < 201901, b.fee, 0 ) ) start_paid_after_fee, " +
                "  0 cur_paid_pre_y_fee, " +
                "  0 cur_paid_pre_m_fee, " +
                "  0 cur_paid_cur_fee, " +
                "  0 cur_paid_pre_cur_fee, " +
                "  0 cur_paid_pre_tax_fee, " +
                "  0 cur_paid_after_fee, " +
                "  0 cur_add_pre_paid_pre_fee, " +
                "  SUM( IF ( b.billing_cycle BETWEEN 201901 AND 201912 AND A.ACCOUNT_CYCLE BETWEEN 201901 AND 201912, b.fee, 0 ) ) cur_add_pre_paid_cur_fee, " +
                "  SUM( IF ( b.billing_cycle BETWEEN 201901 AND 201912 AND A.ACCOUNT_CYCLE BETWEEN 201901 AND 201912, b.tax_fee, 0 ) ) cur_add_pre_paid_tax_fee, " +
                "  SUM( IF ( b.billing_cycle > 201912 AND A.ACCOUNT_CYCLE BETWEEN 201901 AND 201912, b.fee, 0 ) ) cur_add_pre_paid_after_fee, " +
                "  0 end_owes_pre_y_fee, " +
                "  0 end_owes_pre_m_fee, " +
                "  0 end_owes_cur_fee, " +
                "  0 end_owes_total_fee  " +
                " FROM " +
                "   tb_uhome_pay_log A " +
                "  INNER JOIN  tb_uhome_pay_log_detail B ON A.PAY_SERIAL_NBR = B.PAY_SERIAL_NBR " +
                "  LEFT JOIN TB_UHOME_HOUSE C ON B.HOUSE_ID = C.HOUSE_ID " +
                "  LEFT JOIN TB_UHOME_BUILD D ON C.BUILD_ID = D.BUILD_ID " +
                "  LEFT JOIN TB_UHOME_STAGE S ON S.STAGE_ID = D.STAGE_ID " +
                "  LEFT JOIN CUSTOMER E ON E.CUST_ID = B.CUST_ID " +
                "  LEFT JOIN TB_UHOME_FEE_ITEM_TYPE F ON F.FEE_ITEM_TYPE_ID = B.FEE_ITEM_TYPE_ID  " +
                " WHERE " +
                "  A.COMMUNITY_ID = 67  " +
                "  AND A.PAY_CYCLE < 201901 AND B.BILLING_CYCLE >= 201901  " +
                "  AND B.FEE_ITEM_TYPE_ID != 999  " +
                " GROUP BY " +
                "  communityId " +
                "   UNION ALL " +
                " SELECT " +
                "  A.COMMUNITY_ID AS communityId, " +
                "  S.STAGE_ID AS stageId, " +
                "  B.HOUSE_STATUS AS houseStatus, " +
                "  B.HOUSE_STATUS_TYPE AS houseStatusType, " +
                "  S.STAGE_NAME AS stageName, " +
                "  D.BUILD_ID AS buildId, " +
                "  D.NAME AS buildName, " +
                "  C.UNIT_ID AS unitId, " +
                "  C.UNIT AS unitName, " +
                "  C.HOUSE_ID AS houseId, " +
                "  C.HOUSE_NAME AS houseName, " +
                "  B.CUST_ID AS custId, " +
                "  IFNULL( E.CUST_NAME, '业主' ) AS custName, " +
                "  B.FEE_ITEM_TYPE_ID AS feeItemTypeId, " +
                "  F.FEE_ITEM_TYPE_NAME AS feeItemTypeName, " +
                "  B.ACCT_HOUSE_CODE AS acctHouseCode, " +
                "  B.BILL_AREA AS billArea, " +
                "  SUM( IF ( b.billing_cycle < 201901 AND B.ACCOUNT_CYCLE < 201901, b.fee, 0 ) ) begin_owes_pre_y_fee, " +
                "  SUM( IF ( b.billing_cycle >= 201901 AND b.billing_cycle < 201901 AND B.ACCOUNT_CYCLE < 201901, b.fee, 0 ) ) begin_owes_pre_m_fee, " +
                "  0 cur_adjust_pre_fee, " +
                "  0 cur_adjust_cur_y_pre_fee, " +
                "  0 cur_adjust_pre_y_pre_fee, " +
                "  0 cur_sat_receivable_fee, " +
                "  0 cur_adjust_cur_fee, " +
                "  0 cur_discount_fee, " +
                "  0 cur_add_receivable_fee, " +
                "  SUM( IF ( b.billing_cycle < 201901 AND B.ACCOUNT_CYCLE < 201901, b.fee, 0 ) ) cur_total_receivable_fee, " +
                "  0 pre_paid_cur_fee, " +
                "  0 pre_paid_tax_fee, " +
                "  0 pre_paid_after_fee, " +
                "  0 start_paid_after_fee, " +
                "  0 cur_paid_pre_y_fee, " +
                "  0 cur_paid_pre_m_fee, " +
                "  0 cur_paid_cur_fee, " +
                "  0 cur_paid_pre_cur_fee, " +
                "  0 cur_paid_pre_tax_fee, " +
                "  0 cur_paid_after_fee, " +
                "  0 cur_add_pre_paid_pre_fee, " +
                "  0 cur_add_pre_paid_cur_fee, " +
                "  0 cur_add_pre_paid_tax_fee, " +
                "  0 cur_add_pre_paid_after_fee, " +
                "  0 end_owes_pre_y_fee, " +
                "  0 end_owes_pre_m_fee, " +
                "  0 end_owes_cur_fee, " +
                "  0 end_owes_total_fee  " +
                " FROM " +
                "   tb_uhome_pay_log A " +
                "  INNER JOIN  tb_uhome_pay_log_detail B ON A.PAY_SERIAL_NBR = B.PAY_SERIAL_NBR " +
                "  LEFT JOIN TB_UHOME_HOUSE C ON B.HOUSE_ID = C.HOUSE_ID " +
                "  LEFT JOIN TB_UHOME_BUILD D ON C.BUILD_ID = D.BUILD_ID " +
                "  LEFT JOIN TB_UHOME_STAGE S ON S.STAGE_ID = D.STAGE_ID " +
                "  LEFT JOIN CUSTOMER E ON E.CUST_ID = B.CUST_ID " +
                "  LEFT JOIN TB_UHOME_FEE_ITEM_TYPE F ON F.FEE_ITEM_TYPE_ID = B.FEE_ITEM_TYPE_ID  " +
                " WHERE " +
                "  A.COMMUNITY_ID = 67  " +
                "  AND A.ACCOUNT_CYCLE >= 201901  " +
                "  AND B.FEE_ITEM_TYPE_ID != 999  " +
                " GROUP BY " +
                "  communityId " +
                "   UNION ALL " +
                " SELECT " +
                "  A.COMMUNITY_ID AS communityId, " +
                "  S.STAGE_ID AS stageId, " +
                "  A.HOUSE_STATUS AS houseStatus, " +
                "  A.HOUSE_STATUS_TYPE AS houseStatusType, " +
                "  S.STAGE_NAME AS stageName, " +
                "  D.BUILD_ID AS buildId, " +
                "  D.NAME AS buildName, " +
                "  C.UNIT_ID AS unitId, " +
                "  C.UNIT AS unitName, " +
                "  C.HOUSE_ID AS houseId, " +
                "  C.HOUSE_NAME AS houseName, " +
                "  A.CUST_ID AS custId, " +
                "  IFNULL( E.CUST_NAME, '业主' ) AS custName, " +
                "  A.FEE_ITEM_TYPE_ID AS feeItemTypeId, " +
                "  F.FEE_ITEM_TYPE_NAME AS feeItemTypeName, " +
                "  A.ACCT_HOUSE_CODE AS acctHouseCode, " +
                "  A.BILL_AREA AS billArea, " +
                "  SUM( IF ( a.billing_cycle < 201901 AND A.ACCOUNT_CYCLE < 201901, a.fee, 0 ) ) begin_owes_pre_y_fee, " +
                "  SUM( IF ( a.billing_cycle >= 201901 AND a.billing_cycle < 201901 AND A.ACCOUNT_CYCLE < 201901, a.fee, 0 ) ) begin_owes_pre_m_fee, " +
                "  0 cur_adjust_pre_fee, " +
                "  0 cur_adjust_cur_y_pre_fee, " +
                "  0 cur_adjust_pre_y_pre_fee, " +
                "  0 cur_sat_receivable_fee, " +
                "  0 cur_adjust_cur_fee, " +
                "  0 cur_discount_fee, " +
                "  0 cur_add_receivable_fee, " +
                "  SUM( IF ( a.billing_cycle < 201901 AND A.ACCOUNT_CYCLE < 201901, a.fee, 0 ) ) cur_total_receivable_fee, " +
                "  0 pre_paid_cur_fee, " +
                "  0 pre_paid_tax_fee, " +
                "  0 pre_paid_after_fee, " +
                "  0 start_paid_after_fee, " +
                "  0 cur_paid_pre_y_fee, " +
                "  0 cur_paid_pre_m_fee, " +
                "  0 cur_paid_cur_fee, " +
                "  0 cur_paid_pre_cur_fee, " +
                "  0 cur_paid_pre_tax_fee, " +
                "  0 cur_paid_after_fee, " +
                "  0 cur_add_pre_paid_pre_fee, " +
                "  0 cur_add_pre_paid_cur_fee, " +
                "  0 cur_add_pre_paid_tax_fee, " +
                "  0 cur_add_pre_paid_after_fee, " +
                "  SUM( IF ( a.billing_cycle < 201901, a.fee, 0 ) ) end_owes_pre_y_fee, " +
                "  SUM( IF ( a.billing_cycle >= 201901 AND a.billing_cycle < 201901, a.fee, 0 ) ) end_owes_pre_m_fee, " +
                "  SUM( IF ( a.billing_cycle BETWEEN 201901 AND 201912, a.fee, 0 ) ) end_owes_cur_fee, " +
                "  SUM( a.fee ) end_owes_total_fee  " +
                " FROM " +
                "   tb_uhome_acct_item_owes A " +
                "  LEFT JOIN TB_UHOME_HOUSE C ON A.HOUSE_ID = C.HOUSE_ID " +
                "  LEFT JOIN TB_UHOME_BUILD D ON C.BUILD_ID = D.BUILD_ID " +
                "  LEFT JOIN TB_UHOME_STAGE S ON S.STAGE_ID = D.STAGE_ID " +
                "  LEFT JOIN CUSTOMER E ON E.CUST_ID = A.CUST_ID " +
                "  LEFT JOIN TB_UHOME_FEE_ITEM_TYPE F ON F.FEE_ITEM_TYPE_ID = A.FEE_ITEM_TYPE_ID  " +
                " WHERE " +
                "  A.COMMUNITY_ID = 67  " +
                "  AND A.BILLING_CYCLE <= 201912  " +
                "  AND A.PAY_LIMIT_ID < 30  " +
                " GROUP BY " +
                "  communityId " +
                "  " +
                ") t " +
                " LEFT JOIN profession_type pt1 ON pt1.category_code = 50  " +
                " AND pt1.type_code = t.houseStatus " +
                " LEFT JOIN profession_type pt2 ON pt2.category_code = 40  " +
                " AND pt2.type_code = t.houseStatusType  " +
                "GROUP BY " +
                " communityId";

        String sql3 = " select first(build_name),build_id from TB_UHOME_ACCT_ITEM where house_id=295 group by build_id";

        Dataset<Row> nameDf = spark.sql(sql3);
        nameDf.show();
    }

}
