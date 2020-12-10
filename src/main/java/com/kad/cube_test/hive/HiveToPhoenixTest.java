package com.kad.cube_test.hive;

import com.ctrip.framework.apollo.ConfigFile;
import com.ctrip.framework.apollo.ConfigService;
import com.ctrip.framework.apollo.core.enums.ConfigFileFormat;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.jdbc.catalog.JdbcCatalog;
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

public class HiveToPhoenixTest {
    private static ParameterTool config;
    private static Logger LOG = LoggerFactory.getLogger(HiveToPhoenixTest.class);
    public static void main(String[] args) throws IOException, TableNotExistException {
        // 创建处理环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 创建表执行环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        loadConfig(env);                // 加载全局配置
        initHiveCatalog(tableEnv);      // 初始化 Hive Catalog
        initPhoenixCatalog(tableEnv);   // 初始化 Phoenix Catalog

        String phoenixCatalog = "phoenix";
        String phoenixDataBase = "cube_phx";
        String phoenixTable = "ods_om_om_orderdetail";

        String[] castFieldArray = convertCastFields(tableEnv, phoenixCatalog, phoenixDataBase, phoenixTable);
        String castFields = String.join(",", castFieldArray);

        String sql = "SELECT *, LOCALTIMESTAMP as data_modified_datetime, cast(null as TINYINT) as is_delete, 'I' as data_op_type, cast(ordertime as TIMESTAMP) as data_op_ts "
                + " FROM "
                + " `hive`.`myhive`.`ods_om_om_orderdetail` "
                + " WHERE p_day between '2020-05-01' and '2020-09-31'";
        Table table = tableEnv.sqlQuery(sql);
        tableEnv.createTemporaryView("hive_order_detail", table);

        String insertPhoenixSql = "UPSERT INTO `phoenix`.`cube_phx`.ods_om_om_orderdetail SELECT "
                + castFields
                + " FROM "
                + " hive_order_detail";

        StatementSet statementSet = tableEnv.createStatementSet();
        statementSet.addInsertSql(insertPhoenixSql);
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

    private static void loadConfig(ExecutionEnvironment streamEnv) throws IOException {
        // 获取配置，注册到全局
        ConfigFile configFile = ConfigService.getConfigFile("cube", ConfigFileFormat.Properties);
        config = ParameterTool.fromPropertiesFile(new ByteArrayInputStream(configFile.getContent().getBytes()));
        streamEnv.getConfig().setGlobalJobParameters(config);
    }

    private static void initPhoenixCatalog(TableEnvironment tableEnv) {
        String catalog = "phoenix";
        String defaultDatabase = "cube_phx";
        String username = config.get("cube.phoenix.username");
        String password = config.get("cube.phoenix.password");
//        String baseUrl =  config.get("cube.phoenix.jdbcUrl");
        String baseUrl = "jdbc:phoenix:cdh2.360kad.com,cdh4.360kad.com,cdh5.360kad.com:2181;autoCommit=true";
//        String baseUrl = "jdbc:phoenix:node01,node02,node03:2181;autoCommit=true";
        JdbcCatalog phoenixCatalog = new JdbcCatalog(catalog, defaultDatabase, username, password, baseUrl);
        tableEnv.registerCatalog("phoenix", phoenixCatalog);
    }

    private static void initHiveCatalog(TableEnvironment tableEnv) {
        String name = "hive";
        String defaultDatabase = "myhive";
        String hiveConfDir = "src/main/resources"; // a local path
        String version = "2.1.1";
        HiveCatalog hiveCatalog = new HiveCatalog(name, defaultDatabase, hiveConfDir, version);
        tableEnv.registerCatalog("hive", hiveCatalog);
    }
}
