package com.kad.cube_test.phoenix;

import com.kad.cube_test.utils.PhoenixJdbcUtils;

import java.sql.SQLException;

/**
 *  测试 Phoenix Jdbc 插入数据
 */
public class PhoenixUpsertTest {
    public static void main(String[] args) throws SQLException {
        PhoenixJdbcUtils phoenixJdbcUtils = new PhoenixJdbcUtils();
        // sql 语句
        String ordercode = "'1549725288'";
        String cuscode = "'1543091'";
        String cusname = "'zsfor'";
        Integer orderstatus = 3;

        String feilds = String.join(",", ordercode, cuscode, cusname, orderstatus.toString());
        System.out.println(feilds);

        String sql = "upsert into \"cube\".\"ods_om_om_order_test\" " + " (ordercode, cuscode, cusname, orderstatus) "
                + " values ("+ feilds +") ";

        // 插入更新数据
        phoenixJdbcUtils.upsertData(sql);
    }
}
