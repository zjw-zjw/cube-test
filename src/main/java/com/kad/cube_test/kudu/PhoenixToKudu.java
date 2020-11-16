package com.kad.cube_test.kudu;

import com.ctrip.framework.apollo.ConfigFile;
import com.ctrip.framework.apollo.ConfigService;
import com.ctrip.framework.apollo.core.enums.ConfigFileFormat;
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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class PhoenixToKudu {
    private static ParameterTool config;

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
        Table sqlQuery = tableEnv.sqlQuery("select * from phx_table");
        tableEnv.toAppendStream(sqlQuery, Row.class).print();

        List<String> strings = tableEnv.getCatalog("kudu").get().listTables("default_database");
        System.out.println(strings.toString());

//        CatalogBaseTable table = tableEnv.getCatalog("kudu").get().getTable(new ObjectPath("default_database", "impala::test.date_test2"));
//        TableSchema schema = table.getSchema();
//        System.out.println(schema);

        streamEnv.execute();

        PhoenixSinkToKudu(tableEnv);

    }

    private static void PhoenixSinkToKudu(StreamTableEnvironment tableEnv) throws Exception {
        String[] convertFields = ConvertFields(tableEnv, "kudu", "test", "date_test2");
        String convertFieldNames = String.join(",", convertFields);
        System.out.println(Arrays.toString(convertFields));



        String insertKuduSql = "UPSERT INTO `kudu`.`default_database`.`impala::test.date_test2` SELECT "
                +convertFieldNames
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

//
        CatalogBaseTable table = tableEnv.getCatalog("kudu").get().getTable(new ObjectPath("default_database", "impala::test.date_test2"));
        TableSchema schema = table.getSchema();

        //获取表字段
        String[] fields = schema.getFieldNames();
        //获取字段类型
        DataType[] fieldDataTypes = schema.getFieldDataTypes();
        //获取字段数量
        int fieldsCount = schema.getFieldNames().length;

        String[] convertField = new String[fieldsCount];
        //类型转换
        for (int i = 0; i < fieldsCount; i++) {
            for (DataType fieldDataType : fieldDataTypes) {
                if (schema.getFieldDataType(fields[i]).get().toString().equals(fieldDataType.toString())) {
                    if (fieldDataType.toString().contains("NOT NULL")) {
                        String field = fieldDataType.toString().split(" ")[0];
                        String type = "CAST(" + fields[i] + " AS " + field + " )";
                        convertField[i] = type;
                    } else {
                        String field = fieldDataType.toString();
                        String type = "CAST(" + fields[i] + " AS " + field + " )";
                        convertField[i] = type;
                    }
                    break;
                }
            }
        }
        return convertField;
    }


}
