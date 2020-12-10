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
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 *  Ods层 T+1离线同步作业
 *  Phoenix -> Hive分区表
 */
public class OdsPhoenixToHivePartitionCommonJob {
    private static ParameterTool config;
    private static Logger log = LoggerFactory.getLogger(OdsPhoenixToHivePartitionCommonJob.class);
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

        String hiveDatabase     = args[0];  // hive库
        String hiveTable        = args[1];  // hive表
        String phoenixDatabase  = args[2];  // Phoenix库
        String phoenixTable     = args[3];  // Phoenix表
        String partitionTime    = args[4];  // 分区时间
        String hiveCatalog = "hive";
        String phoenixCatalog = "phoenix";

        // 获取同步需要进行时间判断的字段( apollo 配置)
//        String[] etlFields = config.get(phoenixDatabase + "." + phoenixTable).split(",");
        String[] etlFields = "data_modified_datetime,createdate".split(",");

        // 获取Hive 字段及类型映射
        String[] castFieldsArray = convertCastFields(tableEnv, hiveCatalog, hiveDatabase, hiveTable);
        String castFieldsStr = String.join(",", castFieldsArray);
        // System.out.println(castFieldsStr);

        insertToHive(tableEnv, hiveCatalog, hiveDatabase, hiveTable, phoenixCatalog, phoenixDatabase, phoenixTable, etlFields, castFieldsStr, partitionTime);
    }

    private static void insertToHive(TableEnvironment tableEnv, String hiveCatalog, String hiveDatabase, String hiveTable,
                                     String phoenixCatalog, String phoenixDatabase, String phoenixTable,
                                     String[] etlFields, String castFieldsStr, String partitionTime) {
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        StatementSet statementSet = tableEnv.createStatementSet();
        String insertHiveSql_format = "INSERT OVERWRITE %s.%s.%s PARTITION(p_day='%s') SELECT "
                + castFieldsStr
                + " FROM %s.%s.%s " +
                " WHERE ";
        for (int i=0; i < etlFields.length; i++) {
            if (i < etlFields.length-1) {
                insertHiveSql_format = insertHiveSql_format + " date_format(" + etlFields[i] + ", 'yyyy-MM-dd') = '"+partitionTime+"'" + " OR";
            } else {
                insertHiveSql_format = insertHiveSql_format + " date_format(" + etlFields[i] + ", 'yyyy-MM-dd') = '"+partitionTime+"'";
            }
        }

        String insertHiveSql = String.format(insertHiveSql_format,
                hiveCatalog, hiveDatabase, hiveTable,
                partitionTime,
                phoenixCatalog, phoenixDatabase, phoenixTable);
        statementSet.addInsertSql(insertHiveSql);
        statementSet.execute();
        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
    }

    private static String[] convertCastFields(TableEnvironment tableEnv, String catalog, String database, String tableName) throws TableNotExistException {
        CatalogBaseTable hiveTable = tableEnv.getCatalog(catalog).get().getTable(new ObjectPath(database, tableName));
        TableSchema schema = hiveTable.getSchema();
        String[] fieldNames = schema.getFieldNames();
        DataType[] fieldDataTypes = schema.getFieldDataTypes();
        List<String> castFieldList = new ArrayList<>();
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
        String defaultDatabase = "cube_phx";
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
        String hiveConfDir = "src/main/resources"; // hive-site.xml, a local path
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
