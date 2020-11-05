package com.kad.cube_test.phoenix;


import com.kad.cube_test.utils.PhoenixJdbcUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PhoenixJdbcDemo {
    public static void main(String[] args) throws Exception{

        // 获取JDBC工具类
        PhoenixJdbcUtils phoenixJdbcUtils = new PhoenixJdbcUtils();
        // 查询语句
        String sql =  "select ordercode, cuscode, cusname "
                + " from \"cube\".\"ods_om_om_order_test\" "
                + " where ordercode = ? ";

        String ordercode = "1549725408";

        // 执行参数
        ArrayList<Object> params = new ArrayList<>();
        params.add(ordercode);

        // 查询
        List<Map<String, Object>> result = phoenixJdbcUtils.findResult(sql, params);
        for (Map map: result) {
            System.out.println(map);
        }
    }


    /**
     * 查询数据
     */
    public static void queryData() throws Exception{
        // 1.加载驱动
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        // 2. 创建连接
        String url = "jdbc:phoenix:cdh2.360kad.com:2181";
        Connection connection = DriverManager.getConnection(url, "", "");
        // 3.创建Statement对象
        Statement statement = connection.createStatement();
        // 4.执行SQL语句，获取ResultSet结果集
        String sql =
                "select id, cuscode, createdate "
                + "from \"cube\".\"ods_om_om_order\" "
                + "WHERE CREATEDATE BETWEEN '2019-07-02' AND '2020-07-19' "
                + "limit 10";
        ResultSet resultSet = statement.executeQuery(sql);
        // 5.遍历结果集
        while (resultSet.next()) {
            String id = resultSet.getString("ID");
            String cuscode= resultSet.getString("CUSCODE");
            String createDate = resultSet.getString("CREATEDATE");

            System.out.println("id:" + id + ", cuscode:" + cuscode + ", createDate:" + createDate);
        }
        // 6.关闭连接
        resultSet.close();
        statement.close();
        connection.close();
    }

    /**
     *  建立二级索引
     */
    public static void createSecondIndex() throws Exception{
        // 1.加载驱动
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        // 2. 创建连接
        String url = "jdbc:phoenix:cdh2.360kad.com:2181";
        Connection connection = DriverManager.getConnection(url, "", "");
        // 3.创建Statement对象
        Statement statement = connection.createStatement();
        // 4.执行SQL语句，获取ResultSet结果集
        //  全局索引必须是查询语句中所有列都包含在全局索引中它才会生效。
        //  使用覆盖索引可以优化
        String sql =
                "CREATE INDEX IF NOT EXISTS \"idx_ods_om_om_order_cuscode\" ON \"cube\".\"ods_om_om_order\"(\"data\".\"CREATEDATE\" desc) include (\"data\".\"CUSCODE\");";
        boolean flag = statement.execute(sql);
        statement.close();
        connection.close();
    }

    public static void getConection() {

    }
}
