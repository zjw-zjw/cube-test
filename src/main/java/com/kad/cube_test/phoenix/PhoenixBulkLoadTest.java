package com.kad.cube_test.phoenix;

import com.ctrip.framework.apollo.ConfigFile;
import com.ctrip.framework.apollo.ConfigService;
import com.ctrip.framework.apollo.core.enums.ConfigFileFormat;
import com.kad.cube_test.kudu.OfflineBatchToKudu;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.catalog.JdbcCatalog;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.types.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;

/**
 *  读取 hive 数据保存为csv文件。存储在hdfs路径下，再使用 bulk load 脚本 导入数据进 Phoenix表
 */
public class PhoenixBulkLoadTest {
    private static ParameterTool config;
    private static Logger LOG = LoggerFactory.getLogger(PhoenixBulkLoadTest.class);
    public static void main(String[] args) throws IOException, TableNotExistException {
        // 创建处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 创建表执行环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);
        Configuration configuration = tableEnv.getConfig().getConfiguration();
        configuration.setBoolean("table.dynamic-table-options.enabled", true);

        loadConfig(env);                // 加载全局配置
        initHiveCatalog(tableEnv);      // 初始化 Hive Catalog
        initPhoenixCatalog(tableEnv);   // 初始化 Phoenix Catalog

        // 打印测试
        tableEnv.useCatalog("phoenix");
        String printSql = "CREATE TABLE `default_catalog`.`default_database`.`print_table` WITH (\n" +
                " 'connector' = 'print'\n" +
                ") LIKE `ods_om_om_order_test` ( EXCLUDING OPTIONS )";
        tableEnv.executeSql(printSql);
        tableEnv.useCatalog("default_catalog");

        // sink 到 hdfs
        tableEnv.useCatalog("phoenix");
        String sinkHdfs = "CREATE TABLE `default_catalog`.`default_database`.`fs_table` "+
                "   WITH (\n" +
                "  'connector'  =   'filesystem',\n" +
                "  'path'       =   'file:///D:/tmp/phoenix/ods_om_om_order_test/',\n" +
//                "  'path'       =   'hdfs://cdh1.360kad.com:65522/tmp/phoenix/ods_om_om_order_test',\n" +
                "  'format'     =   'csv',\n" +
                "  'sink.rolling-policy.file-size' = '128MB', \n" +
                "  'sink.rolling-policy.rollover-interval' = '10 min', \n" +
                "  'sink.rolling-policy.check-interval' = '1 min' \n" +
                ") LIKE `ods_om_om_order_test` ( EXCLUDING OPTIONS ) ";
        tableEnv.executeSql(sinkHdfs);
        tableEnv.useCatalog("default_catalog");


        /**
         *  2020-10-16 之前
         *  读取 Hive表数据，转换为Phoenix字段类型并 以 csv格式 写入到 hdfs
         */
        String[] castFieldsArray = convertCastFields(tableEnv, "phoenix", "test", "ods_om_om_order_test");
        String castFields = String.join(",", castFieldsArray);
        System.out.println(castFields);

        String sql = "SELECT *, LOCALTIMESTAMP as data_modified_datetime, 'I' as data_op_type, cast(createdate as TIMESTAMP) as data_op_ts "
                + " FROM "
                + " `myhive`.`test_hive`.`ods_om_om_order_test` "
                + " WHERE SUBSTR(createdate, 1, 10) <= '2020-10-16' and SUBSTR(createdate, 1, 10) >= '2020-04-01' ";
        Table table = tableEnv.sqlQuery(sql);
        tableEnv.createTemporaryView("hive_order", table);


        String insertPrintSql = "INSERT INTO print_table SELECT "
                + castFields
                + " FROM "
                + " hive_order";
        String insertFileSql = "INSERT INTO fs_table SELECT "
                + castFields
                + " FROM "
                + " hive_order";
        String insertPhoenixSql = "UPSERT INTO `phoenix`.`test`.ods_om_om_order_test SELECT "
                + castFields
                + " FROM "
                + " hive_order";
        StatementSet statementSet = tableEnv.createStatementSet();
//        statementSet.addInsertSql(insertFileSql);
        statementSet.addInsertSql(insertPrintSql);
//        statementSet.addInsertSql(insertPhoenixSql);
        statementSet.execute();
    }

    private static String[] convertCastFields(TableEnvironment tableEnv, String catalog, String database, String tableName) throws TableNotExistException {
        CatalogBaseTable table = tableEnv.getCatalog(catalog).get().getTable(new ObjectPath(database, tableName));
        TableSchema schema = table.getSchema();
        String[] fieldNames = schema.getFieldNames();
        DataType[] fieldDataTypes = schema.getFieldDataTypes();

        ArrayList<String> castFieldList = new ArrayList<>();
        for (String fieldName : fieldNames) {
            for (DataType fieldDataType : fieldDataTypes) {
                if(schema.getFieldDataType(fieldName).get().toString().equals(fieldDataType.toString())) {
                    String typeStr = fieldDataType.toString();
                    if (typeStr.contains("NOT NULL")){
                        String type = typeStr.split(" ")[0];
                        String castFieldStr = "CAST(" + fieldName + " AS " + type + ") AS " + fieldName;
                        castFieldList.add(castFieldStr);
                    } else {
                        String castFieldStr = "CAST(" + fieldName + " AS " + typeStr + ") AS " + fieldName;
                        castFieldList.add(castFieldStr);
                    }
                    break;
                }
            }
        }
        return castFieldList.toArray(new String[castFieldList.size()]);
    }

    private static void loadConfig(StreamExecutionEnvironment streamEnv) throws IOException {
        // 获取配置，注册到全局
        ConfigFile configFile = ConfigService.getConfigFile("cube", ConfigFileFormat.Properties);
        config = ParameterTool.fromPropertiesFile(new ByteArrayInputStream(configFile.getContent().getBytes()));
        streamEnv.getConfig().setGlobalJobParameters(config);
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

    private static void initHiveCatalog(TableEnvironment tableEnv) {
        String name = "myhive";
        String defaultDatabase = "test_hive";
        String hiveConfDir = "src/main/resources"; // a local path
        String version = "2.1.1";
        HiveCatalog hiveCatalog = new HiveCatalog(name, defaultDatabase, hiveConfDir, version);
        tableEnv.registerCatalog("myhive", hiveCatalog);
    }
}
