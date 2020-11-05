package com.kad.cube_test.hive;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class HiveSourceTest {
    private static Logger LOG = LoggerFactory.getLogger(HiveSourceTest.class);

    public static void main(String[] args) throws Exception {
        // 创建流处理环境
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(1);

        // 创建表执行环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, settings);

        initHiveCatalog(tableEnv);
        initSinkPrint(tableEnv);
        initSinkLocalFileSystem(tableEnv);

        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);

        List<String> hiveDatabase = tableEnv.getCatalog("myhive").get().listDatabases();
        for (String s : hiveDatabase) {
            System.out.println("==> " + s);
        }

        TableSchema schema = tableEnv.getCatalog("myhive").get().getTable(new ObjectPath("test", "test_order_base")).getSchema();
        System.out.println("test_order_base: " + schema);

//        tableEnv.toAppendStream(tableEnv.sqlQuery("select * from `myhive`.`test`.stu"), Row.class).print("stu");
//        streamEnv.execute("hive print job");

        StatementSet statementSet = tableEnv.createStatementSet();
        statementSet.addInsertSql("insert into hive_print_sink select * from `myhive`.`test`.stu");
        statementSet.addInsertSql("insert into fs_table select * from `myhive`.`test`.stu");
        statementSet.execute();
    }

    private static void initSinkLocalFileSystem(StreamTableEnvironment tableEnv) {
        String sinkDDL = "CREATE TABLE fs_table (\n" +
                "                id INT,\n" +
                "                name STRING,\n" +
                "                age INT\n" +
                "        ) WITH (\n" +
                "                'connector' = 'filesystem',\n" +
                "                'path' = 'file:///d:/abc/',\n" +
                "                'format' = 'csv'\n" +
                "        )";
        tableEnv.executeSql(sinkDDL);
    }

    private static void initSinkPrint(StreamTableEnvironment tableEnv) {
        String sinkPrintDDL = "CREATE TABLE hive_print_sink (" +
                "id int," +
                "name STRING," +
                "age int" +
                ") " +
                "WITH (" +
                "'connector' = 'print'" +
                ")";
        tableEnv.executeSql(sinkPrintDDL);
    }

    private static void initHiveCatalog(StreamTableEnvironment tableEnv) {
        String name = "myhive";
        String defaultDatabase = "test";
        String hiveConfDir = "src/main/resources"; // a local path
        String version = "2.1.1";
        HiveCatalog hiveCatalog = new HiveCatalog(name, defaultDatabase, hiveConfDir, version);
        tableEnv.registerCatalog("myhive", hiveCatalog);
    }
}
