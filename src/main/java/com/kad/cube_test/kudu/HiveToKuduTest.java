package com.kad.cube_test.kudu;

import com.ctrip.framework.apollo.ConfigFile;
import com.ctrip.framework.apollo.ConfigService;
import com.ctrip.framework.apollo.core.enums.ConfigFileFormat;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.jdbc.catalog.JdbcCatalog;
import org.apache.flink.connectors.kudu.table.KuduCatalog;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;

/**
 *  离线初始化： 读取 Hive DWD 表数据 同步至 Kudu DWD表
 */
public class HiveToKuduTest {
    private static ParameterTool config;
    private static final Logger LOG = LoggerFactory.getLogger(HiveToKuduTest.class);
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment streamEnv = ExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        loadConfig(streamEnv);
        initHiveCatalog(tableEnv);
        initKuduCatalog(tableEnv);

        String kuduDatabase = "impala_kudu";
        String kuduTable = "dwd_order_retail_order_pay_test";
        String hiveDatabase = "myhive";
        String hiveTable = "dwd_order_retail_order_pay_test";
        String kuduCatalog = "kudu";
        String hiveCatalog = "hive";
        String partitionTime1 = "2020-01-01";
        String partitionTime2 = "2020-10-30";

        String[] convertCastFieldsArray = convertCastFields(tableEnv, kuduCatalog, kuduDatabase, kuduTable);
        String convertCastFields = String.join(",", convertCastFieldsArray);

        String insertKuduSql_format = "UPSERT INTO %s.`default_database`.`impala::%s.%s` SELECT "
                + convertCastFields
                + " FROM "
                + " %s.%s.%s "
                + " WHERE p_day between '%s' and '%s'";
        String insertKuduSql = String.format(insertKuduSql_format,
                kuduCatalog, kuduDatabase, kuduTable,
                hiveCatalog, hiveDatabase, hiveTable,
                partitionTime1, partitionTime2
        );
        StatementSet statementSet = tableEnv.createStatementSet();
        statementSet.addInsertSql(insertKuduSql);
        statementSet.execute();
    }

    private static String[] convertCastFields(TableEnvironment tableEnv, String catalog, String database, String tableName) throws TableNotExistException {
        CatalogBaseTable table = tableEnv.getCatalog(catalog).get().getTable(new ObjectPath("default_database", "impala::"+database+"."+tableName));
        TableSchema schema = table.getSchema();
        String[] fieldNames = schema.getFieldNames();
        DataType[] fieldDataTypes = schema.getFieldDataTypes();

        ArrayList<String> castFieldList = new ArrayList<>();
        for (String fieldName : fieldNames) {
            if ( !fieldName.equals("p_day") ) {
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
        }

        return castFieldList.toArray(new String[castFieldList.size()]);
    }

    private static void initHiveCatalog(TableEnvironment tableEnv) {
        String name = "hive";
        String defaultDatabase = "myhive";
        String hiveConfDir = "src/main/resources"; // a local path
        String version = "2.1.1";
        HiveCatalog hiveCatalog = new HiveCatalog(name, defaultDatabase, hiveConfDir, version);
        tableEnv.registerCatalog("hive", hiveCatalog);
    }

    private static void initKuduCatalog(TableEnvironment tableEnv) {
        String KUDU_MASTERS = config.get("cube.kudu.masterAddresses");
        KuduCatalog catalog = new KuduCatalog(KUDU_MASTERS);
        tableEnv.registerCatalog("kudu", catalog);
    }

    private static void initPhoenixCatalog(StreamTableEnvironment tableEnv) {

        String name = "phoenix";
        String defaultDatabase = "test";
        String username = config.get("cube.phoenix.username");
        String password = config.get("cube.phoenix.password");
//        String baseUrl = config.get("cube.phoenix.jdbcUrl");

        String baseUrl = "jdbc:phoenix:cdh2.360kad.com,cdh4.360kad.com,cdh5.360kad.com:2181;autoCommit=true;";
        JdbcCatalog catalog = new JdbcCatalog(name, defaultDatabase, username, password, baseUrl);
        tableEnv.registerCatalog("phoenix", catalog);
    }

    private static void loadConfig(ExecutionEnvironment streamEnv) throws IOException {

        ConfigFile configFile = ConfigService.getConfigFile("cube", ConfigFileFormat.Properties);
        config = ParameterTool.fromPropertiesFile(new ByteArrayInputStream(configFile.getContent().getBytes()));
        streamEnv.getConfig().setGlobalJobParameters(config);
    }
}
