package com.kad.cube_test.hive;

import com.ctrip.framework.apollo.ConfigFile;
import com.ctrip.framework.apollo.ConfigService;
import com.ctrip.framework.apollo.core.enums.ConfigFileFormat;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.catalog.JdbcCatalog;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.LocalDate;
import java.time.ZoneId;

public class KafkaSinkToHiveTest {
    private static ParameterTool config;
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSinkToHiveTest.class);

    public static void main(String[] args) throws Exception {
        // 创建流处理环境
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
//        StreamExecutionEnvironment streamEnv =
//                StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        streamEnv.enableCheckpointing(10000);
        streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        streamEnv.setParallelism(1);

        // 创建表执行环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, settings);

        loadConfig(streamEnv);          // 加载全局配置
        initHiveCatalog(tableEnv);      // 初始化 Hive Catalog
        initPhoenixCatalog(tableEnv);   // 初始化 Phoenix Catalog
        initKafkaTable(tableEnv);

//        String hiveTable = "ods_om_om_order_test3";
//        String hiveDatabase = "test_hive";

        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        String dropTable = "DROP TABLE IF EXISTS `myhive`.`test_hive`.fs_table";
        String createHiveTableSql = "CREATE TABLE  `myhive`.`test_hive`.fs_table ( " +
                "ordercode STRING, " +
                "suspenddesc STRING," +
                "lastmodifytime STRING" +
                ") " +
                "PARTITIONED BY (dt STRING) " +
                " STORED AS textfile "+
                " TBLPROPERTIES (" +
                " 'partition.time-extractor.timestamp-pattern' = '$dt 00:00:00', " +
                " 'sink.partition-commit.trigger' = 'partition-time', " +
                " 'sink.partition-commit.delay' = '1 d' ," +
                " 'sink.partition-commit.policy.kind' = 'metastore,success-file'," +
                " 'sink.rolling-policy.file-size' = '1MB', " +
                " 'sink.rolling-policy.rollover-interval' = '2 min', " +
                " 'sink.rolling-policy.check-interval' = '1 min' " +
                ")"
                ;
        tableEnv.executeSql(dropTable);
        tableEnv.executeSql(createHiveTableSql);

        StatementSet statementSet = tableEnv.createStatementSet();
        statementSet.addInsertSql("INSERT INTO  `myhive`.`test_hive`.fs_table SELECT ordercode, suspenddesc, cast(lastmodifytime as STRING), DATE_FORMAT(lastmodifytime,'yyyy-MM-dd') FROM kafka_table");
        statementSet.execute();

        tableEnv.toAppendStream(tableEnv.sqlQuery("select ordercode, suspenddesc, cast(lastmodifytime as STRING), DATE_FORMAT(lastmodifytime,'yyyy-MM-dd') FROM kafka_table"), Row.class).print("kafka");
        streamEnv.execute();
    }

    private static void initKafkaTable(StreamTableEnvironment tableEnv) {
        // 获得前一天0点的毫秒值
        Long START_TIMESTAMP = LocalDate
                .now().minusDays(1).atStartOfDay(ZoneId.systemDefault()).toInstant().toEpochMilli();
        String KAFKA_TABLE_CREATE_SQL_FORMAT =
                "CREATE TABLE kafka_table ( "
                        + " `ordercode` STRING, "
                        + " `suspenddesc` STRING, "
                        + " `lastmodifytime` TIMESTAMP(3), "
                        + " proctime as PROCTIME(),"
                        + " WATERMARK FOR `lastmodifytime` AS `lastmodifytime` - INTERVAL '3' SECOND"
                        + ") WITH ("
                        + "'connector' = 'kafka',"
                        + "'topic' = 'cube_phx_ods_om_om_order',"
                        + "'format' = 'json',"
//                        + "'scan.startup.mode' = 'latest-offset', "  // 读取数据的位置
                        + "'scan.startup.mode' = 'timestamp',"
                        + "'scan.startup.timestamp-millis' = '" + START_TIMESTAMP + "',"
                        + getKafkaProperties()
                        + ")";

        tableEnv.executeSql(KAFKA_TABLE_CREATE_SQL_FORMAT);
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

    private static String getKafkaProperties() {
        String kafkaProperties =
                String.format("'properties.bootstrap.servers' = '%s'", config.get("cube.kafka.bootstrap.servers"));
        return kafkaProperties;
    }

}
