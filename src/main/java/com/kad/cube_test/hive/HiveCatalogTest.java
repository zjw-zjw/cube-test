package com.kad.cube_test.hive;

import com.ctrip.framework.apollo.ConfigFile;
import com.ctrip.framework.apollo.ConfigService;
import com.ctrip.framework.apollo.core.enums.ConfigFileFormat;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.jdbc.catalog.JdbcCatalog;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.Row;

import java.io.ByteArrayInputStream;
import java.io.IOException;

public class HiveCatalogTest {
    private static ParameterTool config;

    public static void main(String[] args) throws Exception {
        // 创建流处理环境
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(1);

        // 创建表执行环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, settings);

        loadConfig(streamEnv);          // 加载全局配置
        initHiveCatalog(tableEnv);      // 初始化 Hive Catalog
        initPhoenixCatalog(tableEnv);   // 初始化 Phoenix Catalog


        String sql = "select *, LOCALTIMESTAMP as data_modified_datetime from `myhive`.`test_hive`.ods_om_om_order_test limit 5001";
        tableEnv.executeSql("CREATE TEMPORARY VIEW om_order_hive AS " + sql);

        // hive 与 Phoenix表join
        String hiveJoinPhxSql = "select\n" +
                "    b.ordercode,\n" +
                "    a.ordercode,\n" +
                "    a.address,\n" +
                "    a.ordertime\n" +
                "from\n" +
                "    `phoenix`.`cube_phx`.`ods_om_om_orderaddress` a\n" +
                "left join\n" +
                "     om_order_hive b\n" +
                "on\n" +
                "    a.ordercode = b.ordercode";
        tableEnv.toRetractStream(tableEnv.sqlQuery(hiveJoinPhxSql), Row.class).print();
        streamEnv.execute();

//        String[] castFieldsArray = convertCastFieldsToPhoenix(tableEnv);
//        String castFields = String.join(",", castFieldsArray);
//        StatementSet statementSet = tableEnv.createStatementSet();
//        String insertPhoenixSql = "UPSERT INTO `phoenix`.`test`.`ods_om_om_order_test` select "+ castFields +" from om_order_hive";
//        statementSet.addInsertSql(insertPhoenixSql);
//        statementSet.execute();

//        tableEnv.toAppendStream(tableEnv.sqlQuery("select " + castFields + " from om_order_hive"), Row.class).print("hive_print");
//        streamEnv.execute();
    }

    private static String[] convertCastFieldsToPhoenix(StreamTableEnvironment tableEnv) throws TableNotExistException {
        CatalogBaseTable table = tableEnv.getCatalog("phoenix").get().getTable(new ObjectPath("test", "ods_om_om_order_test"));
        TableSchema schema = table.getSchema();
        String[] fieldNames = schema.getFieldNames();
        DataType[] fieldDataTypes = schema.getFieldDataTypes();

        String[] castFieldArray = new String[fieldNames.length];
        int i = 0;
        for (String fieldName : fieldNames) {
            for (DataType fieldDataType : fieldDataTypes) {
                if(schema.getFieldDataType(fieldName).get().toString().equals(fieldDataType.toString())) {
                    String typeStr = fieldDataType.toString();
                    if (typeStr.contains("NOT NULL")){
                        String type = typeStr.split(" ")[0];
                        String castFieldStr = "CAST(" + fieldName + " AS " + type + ") AS " + fieldName;
                        castFieldArray[i++] = castFieldStr;
                    } else {
                        String castFieldStr = "CAST(" + fieldName + " AS " + typeStr + ") AS " + fieldName;
                        castFieldArray[i++] = castFieldStr;
                    }
                    break;
                }
            }
        }
        return castFieldArray;
    }


    private static void loadConfig(StreamExecutionEnvironment streamEnv) throws IOException {
        // 获取配置，注册到全局
        ConfigFile configFile = ConfigService.getConfigFile("cube", ConfigFileFormat.Properties);
        config = ParameterTool.fromPropertiesFile(new ByteArrayInputStream(configFile.getContent().getBytes()));
        streamEnv.getConfig().setGlobalJobParameters(config);
    }

    private static void initPhoenixCatalog(StreamTableEnvironment tableEnv) {
        String catalog = "phoenix";
        String defaultDatabase = "cube_phx";
        String username = config.get("cube.phoenix.username");
        String password = config.get("cube.phoenix.password");
//        String baseUrl =  config.get("cube.phoenix.jdbcUrl");
        String baseUrl = "jdbc:phoenix:cdh2.360kad.com,cdh4.360kad.com,cdh5.360kad.com:2181;autoCommit=true";

        JdbcCatalog phoenixCatalog = new JdbcCatalog(catalog, defaultDatabase, username, password, baseUrl);
        tableEnv.registerCatalog("phoenix", phoenixCatalog);
    }

    private static void initHiveCatalog(StreamTableEnvironment tableEnv) {
        String name = "myhive";
        String defaultDatabase = "test_hive";
        String hiveConfDir = "src/main/resources"; // a local path
        String version = "2.1.1";
        HiveCatalog hiveCatalog = new HiveCatalog(name, defaultDatabase, hiveConfDir, version);
        tableEnv.registerCatalog("myhive", hiveCatalog);
    }

//    private static void initStreamTableEnv() {
//        streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
//        streamEnv.setParallelism(1);
//        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
//        tableEnv = StreamTableEnvironment.create(streamEnv, settings);
//    }
}
