package com.kad.cube_test.api_test;

import com.google.inject.internal.cglib.core.$Constants;
import com.kad.cube_test.model.Teacher;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 *  测试将 java 的 String 类型 插入到 mysql 的date 类型
 */
public class DateTypeTest {
    public static void main(String[] args) {

        Connection conn = null;
        Statement statement = null;
        String url = "jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=utf8";
        try {
            conn = DriverManager.getConnection(url, "root", "123456");
            statement = conn.createStatement();
            String insertSql = "insert into  teacher values ('bbb', '广州', '2020-02-12 22:33:44')";
            statement.executeUpdate(insertSql);

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                statement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
