package com.kad.cube_test.oracle;

import org.apache.flink.connector.jdbc.catalog.JdbcCatalog;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class OracleSourceTest {

    static StreamExecutionEnvironment streamEnv;
    static StreamTableEnvironment tableEnv;
    public static void main(String[] args) {
        initTableEnvironment();

        tableEnv.sqlQuery("select * from wi_color").printSchema();
    }

    private static void initTableEnvironment() {
        streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(1);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        tableEnv = StreamTableEnvironment.create(streamEnv, settings);
    }
}
