package com.kad.cube_test.kafka;


import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.LocalDate;
import java.time.ZoneId;

public class SourceTest {
//    private static ParameterTool config;
//    private static Logger LOG = LoggerFactory.getLogger(SourceTest.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.enableCheckpointing(60 * 60 * 1000);  // 1小时的checkpoint间隔
        streamEnv.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, settings);
        Configuration configuration = tableEnv.getConfig().getConfiguration();
        configuration.setBoolean("table.dynamic-table-options.enabled", true);
//        loadConfig(streamEnv);

        // 获得前一天0点的毫秒值
        Long START_TIMESTAMP = LocalDate
                .now().minusDays(1).atStartOfDay(ZoneId.systemDefault()).toInstant().toEpochMilli();
        String KAFKA_TABLE_CREATE_SQL_FORMAT =
                "CREATE TABLE %s ("
                        + "%s,"
                        + "`data_modified_datetime` TIMESTAMP(3),"
                        + "`data_op_type` STRING,"
                        + "`data_op_ts` TIMESTAMP(3),"
                        + "WATERMARK FOR `data_op_ts` AS `data_op_ts` - INTERVAL '3' SECOND"
                        + ") WITH ("
                        + "'connector' = 'kafka',"
                        + "'topic' = '%s',"
                        + "'format' = 'json',"
                        + "'scan.startup.mode' = 'timestamp',"
                        + "'scan.startup.timestamp-millis' = '" + START_TIMESTAMP + "',"
                        + "'properties.bootstrap.servers' = 'cdh1.360kad.com:9092,cdh2.360kad.com:9092,cdh5.360kad.com:9092'"
                        + ")";

        String fields = String.join(",", new String[] {
                "`ordercode` STRING",          // 订单ID
                "`cuscode` STRING",            // 客户ID
                "`changetype` SMALLINT",        // 换货类型 -> 订单类型
                "`ordersource` SMALLINT",           // 订单来源
                "`shopcode` STRING",           // 下单门店编号或下单第三方店铺编号
                "`salemode` SMALLINT",          // 销售模式
                "`invoicetype` TINYINT",       // 登记发票类型
                "`createdate` TIMESTAMP(3)"    // 下单时间
        });

        String table = "ods_om_om_order_online";
        String topic = "cube_phx_ods_om_om_order";


        String sql = String.format(KAFKA_TABLE_CREATE_SQL_FORMAT, table, fields, topic);
        tableEnv.executeSql(sql);

        // 打印 om order 表信息
        Table tableResult = tableEnv.sqlQuery("select * from ods_om_om_order_online where `data_op_type` = 'I'");
        tableEnv.toAppendStream(tableResult, Row.class).print("order");

        streamEnv.execute("test_job");

    }

//    private static void loadConfig(StreamExecutionEnvironment streamEnv) throws IOException {
//        // 获取配置，注册到全局
//        ConfigFile configFile = ConfigService.getConfigFile("cube", ConfigFileFormat.Properties);
//        config = ParameterTool.fromPropertiesFile(new ByteArrayInputStream(configFile.getContent().getBytes()));
//        streamEnv.getConfig().setGlobalJobParameters(config);
//    }

//    private static String getKafkaProperties() {
//        String kafkaProperties =
//                String.format("'properties.bootstrap.servers' = '%s'", config.get("cube.kafka.bootstrap.servers"));
//        return kafkaProperties;
//    }

}
