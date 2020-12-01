package com.kad.cube_test.kudu;

import com.ctrip.framework.apollo.ConfigFile;
import com.ctrip.framework.apollo.ConfigService;
import com.ctrip.framework.apollo.core.enums.ConfigFileFormat;
import com.kad.cube_test.hive.KafkaSinkToHiveTest;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.jdbc.catalog.JdbcCatalog;
import org.apache.flink.connectors.kudu.table.KuduCatalog;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PhoenixToKudu {
    private static ParameterTool config;
    private static final Logger LOG = LoggerFactory.getLogger(PhoenixToKudu.class);
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, settings);

        loadConfig(streamEnv);
        initPhoenixCatalog(tableEnv);
        initKuduCatalog(tableEnv);

        String sql = "create temporary view phx_table as select * from `phoenix`.`test`.`date_test` ";
        tableEnv.executeSql(sql);
        PhoenixSinkToKudu(tableEnv);

        Table sqlQuery = tableEnv.sqlQuery("select * from phx_table");
        tableEnv.toAppendStream(sqlQuery, Row.class).print();
        streamEnv.execute();
    }

    private static void PhoenixSinkToKudu(StreamTableEnvironment tableEnv) throws Exception {
        String[] convertFields = ConvertFields(tableEnv, "kudu", "test", "date_test2");
        String convertFieldNames = String.join(",", convertFields);
        System.out.println(Arrays.toString(convertFields));

        String insertKuduSql = "UPSERT INTO `kudu`.`default_database`.`impala::test.date_test2` SELECT "
                + convertFieldNames
                + " FROM phx_table";

        StatementSet statementSet = tableEnv.createStatementSet();
        statementSet.addInsertSql(insertKuduSql);
        statementSet.execute();

}


    private static void initKuduCatalog(StreamTableEnvironment tableEnv) {
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

    private static void loadConfig(StreamExecutionEnvironment streamEnv) throws IOException {

        ConfigFile configFile = ConfigService.getConfigFile("cube", ConfigFileFormat.Properties);
        config = ParameterTool.fromPropertiesFile(new ByteArrayInputStream(configFile.getContent().getBytes()));
        streamEnv.getConfig().setGlobalJobParameters(config);
    }

    private static String[] ConvertFields(StreamTableEnvironment tableEnv, String catalogName, String databaseName, String tableName) throws Exception {
        CatalogBaseTable table = tableEnv.getCatalog(catalogName).get().getTable(new ObjectPath("default_database", "impala::" + databaseName + "."+ tableName));
        TableSchema schema = table.getSchema();
        //获取表字段
        String[] fieldNames = schema.getFieldNames();
        //获取字段类型
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
}
