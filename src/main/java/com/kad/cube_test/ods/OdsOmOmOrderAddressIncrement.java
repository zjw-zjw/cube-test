package com.kad.cube_test.ods;

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

/**
 * 同步 Phoenix的零售订单收货地址表的新增变化数据 到 Hive的零售订单收货地址新增变化分区表
 */
public class OdsOmOmOrderAddressIncrement {
    private static ParameterTool config;
    private static Logger log = LoggerFactory.getLogger(OdsOmOmOrderAddressIncrement.class);
    public static void main(String[] args) throws IOException, TableNotExistException {
        // 创建批处理环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 创建表执行环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);


        loadConfig(env);                // 加载全局配置
        initHiveCatalog(tableEnv);      // 初始化 Hive Catalog
        initPhoenixCatalog(tableEnv);   // 初始化 Phoenix Catalog

        String hiveDatabase     = "myhive";
        String hiveTable        = "ods_om_om_orderaddress_incr";
        String phoenixDatabase  = "cube_phx";
        String phoenixTable     = "ods_om_om_orderaddress";
        String partition_time   = args[0];

        String hiveCatalog = "hive";
        String phoenixCatalog = "phoenix";
        String[] castFieldsArray = convertCastFields(tableEnv, hiveCatalog, hiveDatabase, hiveTable);
        String castFieldsStr = String.join(",", castFieldsArray);
        System.out.println(castFieldsStr);

        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        StatementSet statementSet = tableEnv.createStatementSet();
        String insertHiveSql_format = "INSERT OVERWRITE %s.%s.%s PARTITION(p_day='%s') select "
                + castFieldsStr
                + " FROM %s.%s.%s " +
                " WHERE (date_format(data_op_ts, 'yyyy-MM-dd') = '%s' or date_format(ordertime, 'yyyy-MM-dd') = '%s') ";
        String insertHiveSql = String.format(insertHiveSql_format,
                hiveCatalog, hiveDatabase, hiveTable,
                partition_time,
                phoenixCatalog, phoenixDatabase, phoenixTable
                ,partition_time, partition_time);
        statementSet.addInsertSql(insertHiveSql);
        statementSet.execute();
    }

    private static String[] convertCastFields(TableEnvironment tableEnv, String catalog, String database, String tableName) throws TableNotExistException {
        CatalogBaseTable hiveTable = tableEnv.getCatalog(catalog).get().getTable(new ObjectPath(database, tableName));
        TableSchema schema = hiveTable.getSchema();
        String[] fieldNames = schema.getFieldNames();
        DataType[] fieldDataTypes = schema.getFieldDataTypes();

        ArrayList<String> castFieldList = new ArrayList<>();
        for (String fieldName : fieldNames) {
            if ( !"p_day".equals(fieldName) ) {
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

    private static void initPhoenixCatalog(TableEnvironment tableEnv) {
        String catalog = "phoenix";
        String defaultDatabase = "default";
        String username = config.get("cube.phoenix.username");
        String password = config.get("cube.phoenix.password");
        String baseUrl =  config.get("cube.phoenix.jdbcUrl");
//        String baseUrl = "jdbc:phoenix:cdh2.360kad.com,cdh4.360kad.com,cdh5.360kad.com:2181;autoCommit=true";

        JdbcCatalog phoenixCatalog = new JdbcCatalog(catalog, defaultDatabase, username, password, baseUrl);
        tableEnv.registerCatalog("phoenix", phoenixCatalog);
    }

    private static void initHiveCatalog(TableEnvironment tableEnv) {
        String name = "hive";
        String defaultDatabase = "default";
        String hiveConfDir = "src/main/resources"; // a local path
        String version = "2.1.1";
        HiveCatalog hiveCatalog = new HiveCatalog(name, defaultDatabase, hiveConfDir, version);
        tableEnv.registerCatalog("hive", hiveCatalog);
    }

    private static void loadConfig(ExecutionEnvironment env) throws IOException {
        // 获取配置，注册到全局
        ConfigFile configFile = ConfigService.getConfigFile("cube", ConfigFileFormat.Properties);
        config = ParameterTool.fromPropertiesFile(new ByteArrayInputStream(configFile.getContent().getBytes()));
        env.getConfig().setGlobalJobParameters(config);
    }
}
