import com.alibaba.fastjson.JSONObject;
import com.yr.constant.Constants;

import java.util.Map;

/**
 * @author dengbp
 * @ClassName AnalyzeDataTest
 * @Description TODO
 * @date 2020-03-10 18:06
 */
public class AnalyzeDataTest {

    public static void main(String[] args) {
        String data = "{\"database\":\"bill_01\",\"table\":\"tb_uhome_acct_item_owes\",\"type\":\"insert\",\"ts\":1583724393,\"xid\":144638281,\"commit\":true,\"data\":{\"acct_item_id\":64880222,\"COMMUNITY_ID\":67,\"House_id\":112739637,\"Acct_item_type_id\":150002,\"FEE\":15800,\"amount\":0.0000,\"Unit_type\":\"\",\"Billing_cycle\":202004,\"Create_date\":\"2020-03-09 11:26:33\",\"State_date\":\"2020-03-09 03:26:33\",\"bill_date_start\":\"20200401000000\",\"bill_date_end\":\"20200430235959\",\"pay_limit_id\":0,\"State\":\"PNO\",\"lfree\":0,\"obj_id\":64610002,\"cust_id\":110880089,\"bill_Flag\":2,\"notice_flag\":0,\"fee_item_type_id\":64120001,\"tax_rate\":0.00,\"tax_fee\":0.0000,\"lfree_tax_rate\":0.00,\"bill_fee\":15800,\"lfree_begin_date\":\"\",\"lfree_rate_id\":0,\"init_val\":-1.00,\"end_val\":-1.00,\"task_type\":20,\"task_id\":64610002,\"pay_serial_id\":null,\"src_acct_item_id\":64880222,\"lfree_tax_fee\":0.0000,\"lfree_fee\":0,\"bill_obj_type\":1,\"res_inst_id\":112739637,\"real_cycle\":202004,\"rate_str\":\"\",\"real_community_id\":67,\"obj_type\":20,\"outer_bill_id\":-1,\"account_cycle\":202003,\"bill_rule_id\":0,\"build_id\":115989878,\"unit_id\":115989879,\"build_name\":\"ceshi1\",\"unit_name\":\"1dan\",\"house_name\":\"ceshi1_1dan_101\",\"cust_name\":\"3232qwe\",\"fee_item_type_name\":\"免税物业费fcs\",\"res_inst_code\":\"\",\"res_inst_name\":\"ceshi1_1dan_101\",\"obj_code\":\"\",\"obj_name\":\"\",\"receivable_date\":0,\"bill_rule_name\":\"\",\"house_status\":\"\",\"house_status_type\":\"\",\"acct_house_code\":\"\",\"bill_area\":32.7100,\"owes_type_id\":null,\"lfree_hangup_flag\":0,\"bill_contract_id\":-1,\"first_flag\":0,\"cust_main_id\":110880090,\"lease_position\":\"\",\"LOSS_RATE\":\"\",\"RATE\":\"\",\"LOSS_RATE_VALUE\":\"\",\"diff_value\":0.0,\"stage_id\":0,\"stage_name\":null,\"lfee_rate\":0.0000,\"recv_base_date\":0,\"recv_recalc_flag\":0,\"invoice_lock_fee\":0,\"pay_rule_id\":0,\"sys_id\":0,\"unit_str\":\"\"}}";
        JSONObject jsonObject = JSONObject.parseObject(data);
        System.out.println(jsonObject.get("database"));
        System.out.println(jsonObject.get("table"));
        Map map = (Map) jsonObject.get("data");
        System.out.println(map.get(Constants.TB_UHOME_ACCT_ITEM_OWES_PRIMARY_KEY));
        map.forEach((k,v)->{
            System.out.print("k["+k+"]=");
            System.out.print("v["+v+"]");
            System.out.println("");
        });
    }
}
