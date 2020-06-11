/**
 * @author dengbp
 * @ClassName SplitTest
 * @Description TODO
 * @date 2020-03-09 00:31
 */
public class SplitTest {

    public static void main(String[] args) {

        String dd = "bill$$tb_uhome_acct_item$$1111$${\"a1\":\"a1value\",\"a2\":\"a2value\"}";
        System.out.println(dd.split("\\$\\$").length);
    }
}
