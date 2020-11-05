package com.kad.cube_test.phoenix;

import com.kad.cube_test.utils.PhoenixJdbcUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

// 读取 om库 ods_om_om_order_test  测试
public class PhoenixQueryOm {
    public static void main(String[] args) throws Exception {
//        queryData();


//        upsertData();


        // 获取JDBC工具类
        PhoenixJdbcUtils phoenixJdbcUtils = new PhoenixJdbcUtils();
        // 查询语句
        String sql =  "select \"ordercode\", \"detailid\" "
                + " from \"test\".\"ods_om_om_orderdetail\" "
                + " where \"ordercode\" = ? ";

        String ordercode = "1549734518";

        // 执行参数
        ArrayList<Object> params = new ArrayList<>();
        params.add(ordercode);

        // 查询
        List<Map<String, Object>> result = phoenixJdbcUtils.findResult(sql, params);
        for (Map map: result) {
            for (Object o : map.keySet()) {
                System.out.println(o.toString() + ":" + map.get(o));
            }
        }

    }

    private static void upsertData() throws SQLException, ClassNotFoundException {
        // 1.加载驱动
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        // 2. 创建连接
        String url = "jdbc:phoenix:cdh2.360kad.com:2181";
        Connection connection = DriverManager.getConnection(url, "", "");
        connection.setAutoCommit(true);     // 设置自动提交  phoenix jdbc 的Connection默认不是auto commit模式

        // 3.创建Statement对象
        Statement statement = connection.createStatement();
        // 4.执行SQL语句
        String ordercode = "'1549725288'";
        String cuscode = "'1543091'";
        String cusname = "'zsfor'";

        String feilds = String.join(",", ordercode.toString(), cuscode.toString(), cusname);
        System.out.println(feilds);


        String sql = "upsert into \"cube\".\"ods_om_om_order_test\" " + " (ordercode, cuscode, cusname)" + " VALUES( " + feilds + ")";

        statement.execute(sql);


        // phoenix jdbc 的Connection默认不是auto commit模式，跟一般数据库不同。所以要再执行完SQL后显示加上connection.commit()，更新才能提交到数据库。
        // 可以设置 connection.setAutoCommit(true);
//        connection.commit();

        // 6.关闭连接
        statement.close();
        connection.close();
    }

    public static void queryData() throws ClassNotFoundException, SQLException  {
        // 1.加载驱动
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        // 2. 创建连接
        String url = "jdbc:phoenix:cdh2.360kad.com:2181";
        Connection connection = DriverManager.getConnection(url, "", "");
        // 3.创建Statement对象
        Statement statement = connection.createStatement();
        // 4.执行SQL语句，获取ResultSet结果集
        String sql =
                "select ordercode, cuscode, cusname "
                        + " from \"cube\".\"ods_om_om_order_test\" "
                        + " limit 10";
        ResultSet resultSet = statement.executeQuery(sql);
        // 5.遍历结果集
        while (resultSet.next()) {
            String ordercode = resultSet.getString("ORDERCODE");
            String cuscode= resultSet.getString("CUSCODE");
            String cusname = resultSet.getString("CUSNAME");

            System.out.println("ordercode:" + ordercode + ", cuscode:" + cuscode + ", cusname:" + cusname);
        }

        // 6.关闭连接
        resultSet.close();
        statement.close();
        connection.close();
    }
}
