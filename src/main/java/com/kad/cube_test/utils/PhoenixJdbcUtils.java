package com.kad.cube_test.utils;

import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.*;

/**
 *  连接 Phoenix的jdbc工具类
 */
public class PhoenixJdbcUtils {

    private static String USERNAME;
    private static String PASSWORD;
    private static String DRIVER;
    private static String URL;

    // 定义数据库连接
    private Connection connection;
    // 定义sql语句的执行对象
    private PreparedStatement pstmt;
    // 定义查询返回的结果集合
    private ResultSet resultSet;

    static {
        loadConfig();
    }

    /**
     * 加载数据库配置信息
     */
    public static void loadConfig() {

        try {
            InputStream inStream = PhoenixJdbcUtils.class.getResourceAsStream("/phoenixjdbc.properties");
            Properties prop = new Properties();
            prop.load(inStream);
            USERNAME = prop.getProperty("jdbc.username");
            PASSWORD = prop.getProperty("jdbc.password");
            DRIVER = prop.getProperty("jdbc.driver");
            URL = prop.getProperty("jdbc.url");
        } catch (IOException e) {
            throw new RuntimeException("读取数据库配置文件异常！", e);
        }
    }

    public PhoenixJdbcUtils() {
        super();
    }

    /**
     *  初始化连接
     */
    private void initConnection(){
        try {
            //local
            connection = DriverManager.getConnection(URL, USERNAME, PASSWORD);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     *  执行更新插入操作
     * @param sql  sql语句
     * @throws SQLException
     */
    public void upsertData(String sql) {
        if (null == connection) {
            initConnection();
        }

        try {
            Statement statement = connection.createStatement();
            statement.executeUpdate(sql);
            connection.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            // 关闭资源
            releaseConn();
        }
    }


    /**
     * 使用占位符 执行更新插入操作
     * @param sql sql语句
     * @param params 执行参数
     * @return 执行结果
     * @throws SQLException
     */
    public boolean upsertByPreparedStatement(String sql, List<?> params) {
        if (null == connection) {
            initConnection();
        }

        boolean flag = false;
        int result = -1;
        try {
            pstmt = connection.prepareStatement(sql);
            // 填充sql语句中的占位符
            if (params != null && !params.isEmpty()) {
                for (int i = 0; i < params.size(); i++) {
                    pstmt.setObject(i + 1, params.get(i));
                }
            }
            result = pstmt.executeUpdate();
            connection.commit();
            return flag = result > 0;
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            // 关闭资源
            releaseConn();
        }
        return flag;
    }

    /**
     * @param sql
     * @param list
     * 功能介绍：批量更新
     */
    public void batchUpdate(String sql, List<Object[]> list) {

        if(null == connection){
            initConnection();
        }

        try {
            pstmt = connection.prepareStatement(sql);
            //关闭自动提交事务
            connection.setAutoCommit(false);
            //防止内存溢出
            final int batchSize = 1000;
            //记录插入数量
            int count = 0;
            int size = list.size();
            Object[] obj = null;
            for (int i = 0; i < size; i++) {
                obj = list.get(i);
                for (int j = 0; j < obj.length; j++) {
                    pstmt.setObject(j + 1, obj[j]);
                }
                pstmt.addBatch();
                if (++count % batchSize == 0) {
                    pstmt.executeBatch();
                    connection.commit();
                }
            }
            pstmt.executeBatch();
            connection.commit();
            connection.setAutoCommit(true);
        } catch (SQLException e) {
            e.printStackTrace();
            try {
                connection.rollback();
                connection.setAutoCommit(true);
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
        } finally {
            //关闭资源
            releaseConn();
        }
    }

    /**
     * 执行查询操作
     * @param sql sql语句
     * @param params 执行参数
     * @return
     * @throws Exception
     */
    public List<Map<String, Object>> findResult(String sql, List<?> params)  {
        if (null == connection) {
            initConnection();
        }

        List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
        int index = 1;
        try {
            pstmt = connection.prepareStatement(sql);
            if (params != null && !params.isEmpty()) {
                for (int i = 0; i < params.size(); i++) {
                    pstmt.setObject(index++, params.get(i));    // the first parameter is 1, the second is 2, ...
                }
            }
            resultSet = pstmt.executeQuery();
            ResultSetMetaData metaData = resultSet.getMetaData();
            int cols_len = metaData.getColumnCount();
            while (resultSet.next()) {
                Map<String, Object> map = new HashMap<>();
                for (int i = 0; i < cols_len; i++) {
                    String cols_name = metaData.getColumnName(i + 1);
                    Object cols_value = resultSet.getObject(cols_name);     // 通过列名来获取对应的值
                    if (cols_value == null) {
                        cols_value = "";
                    }
                    map.put(cols_name, cols_value);
                }
                list.add(map);
            }

            return list;
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            // 释放资源
            releaseConn();
        }

        return  null;
    }


    /**
     * 释放资源
     */
    public void releaseConn() {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (pstmt != null) {
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
