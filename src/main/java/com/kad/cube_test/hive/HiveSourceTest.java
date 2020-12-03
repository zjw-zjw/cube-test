package com.kad.cube_test.hive;

import com.ctrip.framework.apollo.ConfigFile;
import com.ctrip.framework.apollo.ConfigService;
import com.ctrip.framework.apollo.core.enums.ConfigFileFormat;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.jdbc.catalog.JdbcCatalog;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;

public class HiveSourceTest {
    private static ParameterTool config;
    private static Logger LOG = LoggerFactory.getLogger(HiveSourceTest.class);

    public static void main(String[] args) throws Exception {
        // 创建流处理环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 创建表执行环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        loadConfig(env);                // 加载全局配置
        initHiveCatalog(tableEnv);
        initPhoenixCatalog(tableEnv);       // 初始化 Phoenix Catalog
        initSinkPrint(tableEnv);
        initSinkLocalFileSystem(tableEnv);

        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);

        TableSchema schema = tableEnv.getCatalog("hive").get().getTable(new ObjectPath("myhive", "ods_om_om_order")).getSchema();
        System.out.println("test_order_base: " + schema);

//        tableEnv.toAppendStream(tableEnv.sqlQuery("select * from `hive`.`myhive`.ods_om_om_order limit 10"), Row.class).print("ods_om_om_order");
//        streamEnv.execute("hive print job");

        StatementSet statementSet = tableEnv.createStatementSet();
        statementSet.addInsertSql("insert into hive_print_sink select ordercode, cuscode, p_day from `hive`.`myhive`.ods_om_om_order where p_day >= '2019-10-01' and p_day <= '2019-11-01'");
        statementSet.addInsertSql("insert into fs_table select ordercode, cuscode, p_day from `hive`.`myhive`.ods_om_om_order where p_day >= '2019-10-01' and p_day <= '2019-11-01'");
        statementSet.execute();
    }

    private static void initSinkLocalFileSystem(TableEnvironment tableEnv) {
        String sinkDDL = "CREATE TABLE fs_table (\n" +
                "       ordercode STRING,\n" +
                "       cuscode STRING,\n" +
                "       p_day STRING\n" +
                "        ) WITH (\n" +
                "                'connector' = 'filesystem',\n" +
                "                'path' = 'file:///d:/ods_om_om_order/',\n" +
                "                'format' = 'csv'\n" +
                "        )";
        tableEnv.executeSql(sinkDDL);
    }

    private static void initSinkPrint(TableEnvironment tableEnv) {
        String sinkPrintDDL = "CREATE TABLE hive_print_sink (" +
                "ordercode STRING,\n" +
                "cuscode STRING,\n" +
                "p_day STRING\n" +
                ") " +
                "WITH (" +
                "'connector' = 'print'" +
                ")";
        tableEnv.executeSql(sinkPrintDDL);
    }

    private static void initHiveCatalog(TableEnvironment tableEnv) {
        String name = "hive";
        String defaultDatabase = "myhive";
        String hiveConfDir = "src/main/resources"; // a local path
        String version = "2.1.1";
        HiveCatalog hiveCatalog = new HiveCatalog(name, defaultDatabase, hiveConfDir, version);
        tableEnv.registerCatalog("hive", hiveCatalog);
    }

    private static void initPhoenixCatalog(TableEnvironment tableEnv) {
        String catalog = "phoenix";
        String defaultDatabase = "test";
        String username = config.get("cube.phoenix.username");
        String password = config.get("cube.phoenix.password");
//        String baseUrl =  config.get("cube.phoenix.jdbcUrl");
        String baseUrl = "jdbc:phoenix:cdh2.360kad.com,cdh4.360kad.com,cdh5.360kad.com:2181;autoCommit=true";

        JdbcCatalog phoenixCatalog = new JdbcCatalog(catalog, defaultDatabase, username, password, baseUrl);
        tableEnv.registerCatalog("phoenix", phoenixCatalog);
    }

    private static void loadConfig(ExecutionEnvironment streamEnv) throws IOException {
        // 获取配置，注册到全局
        ConfigFile configFile = ConfigService.getConfigFile("cube", ConfigFileFormat.Properties);
        config = ParameterTool.fromPropertiesFile(new ByteArrayInputStream(configFile.getContent().getBytes()));
        streamEnv.getConfig().setGlobalJobParameters(config);
    }
}
