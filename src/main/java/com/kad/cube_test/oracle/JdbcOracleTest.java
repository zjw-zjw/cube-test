package com.kad.cube_test.oracle;

import com.kad.cube_test.model.OmOrder;
import scala.xml.pull.ExceptionEvent;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JdbcOracleTest {
    public static void main(String[] args) {
        testOracle();
    }

    private static void testOracle() {
        Connection con = null;// 创建一个数据库连接
        PreparedStatement pre = null;// 创建预编译语句对象，一般都是用这个而不用Statement
        ResultSet result = null;// 创建一个结果集对象
        List<OmOrder> list =null;
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");   // 加载Oracle驱动程序
            System.out.println("开始尝试连接数据库！");

            String url = "jdbc:oracle:" + "thin:@192.168.1.23:1521:kaderp";
            String user = "dc";
            String password = "dc360kad";
            con = DriverManager.getConnection(url, user, password);
            System.out.println("连接成功！");

            String sql = "select * from om.OM_ORDER WHERE rownum <=10";
            pre = con.prepareStatement(sql);
            result = pre.executeQuery();

            list = new ArrayList<OmOrder>();

            ResultSetMetaData metaData = result.getMetaData();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                String name = metaData.getColumnName(i);
                int columnType = metaData.getColumnType(i);
                System.out.println("column: " + name + "\ttype: " + columnType);
            }
            while (result.next()) {
                Map<String, Object> map = new HashMap<String, Object>();
                for (int i = 0; i < metaData.getColumnCount(); i++) {
                    String col_name = metaData.getColumnName(i+1);
                    Object col_value = result.getObject(col_name);
                    map.put(col_name, col_value);
                }
                System.out.println(map.toString());
            }
        } catch (Exception e){
            e.printStackTrace();
        } finally {
            try{
                // 逐一将上面的几个对象关闭，因为不关闭的话会影响性能、并且占用资源
                // 注意关闭的顺序，最后使用的最先关闭
                if (result != null)
                    result.close();
                if (pre != null)
                    pre.close();
                if (con != null)
                    con.close();
                System.out.println("数据库连接已关闭！");
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }

        }

    }
}
