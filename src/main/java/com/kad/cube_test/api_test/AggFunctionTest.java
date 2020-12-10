package com.kad.cube_test.api_test;

import com.ctrip.framework.apollo.ConfigFile;
import com.ctrip.framework.apollo.ConfigService;
import com.ctrip.framework.apollo.core.enums.ConfigFileFormat;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.format.DateTimeFormatter;

public class AggFunctionTest {

    private static ParameterTool cubeConfig;
    private static DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd");

    private static final String KEY_DATE_FORMAT = "yyyyMMdd";

    private static final String DEFAULT_TTL = Integer.toString(7 * 24 * 60 * 60);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        // StreamExecutionEnvironment streamEnv =
        // StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        // streamEnv.enableCheckpointing(60 * 60 * 1000);
        CheckpointConfig checkpointConfig = streamEnv.getCheckpointConfig();
        checkpointConfig.setCheckpointInterval(60 * 60 * 1000);
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        streamEnv.setParallelism(1);
        // streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // streamEnv.getConfig().setAutoWatermarkInterval(1000);
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, bsSettings);

        loadConfig(tableEnv);

        registerFunction(tableEnv);

        TableConfig tableConfig = tableEnv.getConfig();
        tableConfig.setIdleStateRetentionTime(Time.hours(24), Time.hours(36));
        Configuration configuration = tableConfig.getConfiguration();
        configuration.setBoolean("table.dynamic-table-options.enabled", true);
        configuration.setBoolean("table.exec.mini-batch.enabled", true);
        configuration.setString("table.exec.mini-batch.allow-latency", "1 s");
        configuration.setLong("table.exec.mini-batch.size", 5000);

        initDataGen(tableEnv);

        String sql1 = "SELECT LOCALTIMESTAMP as `localtime`, to_date(CAST(LOCALTIMESTAMP AS STRING)) as submit_date, cast(5 as DOUBLE) as sale, ts  FROM datagen";

//        String sql2 = "SELECT " +
//                " submit_date, " +
//                " date_format(ts, 'yyyy-MM-dd HH:mm') as ts ,"  +
//                " cast(sum(sale) as DOUBLE) as sale_amount " +
//                " FROM " +
//                " (" + sql1 + ") as t " +
//                " GROUP BY submit_date, date_format(ts, 'yyyy-MM-dd HH:mm') ";
//
//        String sql3 = "SELECT " +
//                " submit_date, " +
//                " SaleAmountMillionFunction(CAST(submit_date as TIMESTAMP(3)), ts, sale_amount) " +
//                " FROM " +
//                " (" + sql2 +") as t " +
//                " GROUP BY submit_date";


        tableEnv.sqlQuery(sql1).printSchema();
        tableEnv.toAppendStream(tableEnv.sqlQuery(sql1), Row.class).print("AAAAA--------------------");
//        tableEnv.toRetractStream(tableEnv.sqlQuery(sql2), Row.class).print("BBBBB---");
//        tableEnv.toRetractStream(tableEnv.sqlQuery(sql3), Row.class).print("CCCCC---");
        streamEnv.execute();
    }

    private static void initDataGen(StreamTableEnvironment tableEnv) {
        String dategen = "CREATE TABLE datagen ( " +
                " submit_datetime bigint ,"+
                " ts AS localtimestamp," +
                " WATERMARK FOR ts AS ts" +
                " ) WITH (" +
                " 'connector' = 'datagen'," +
                " 'rows-per-second'='1'," +
                " 'fields.submit_datetime.kind'='random'," +
                " 'fields.submit_datetime.min'='1607270400000'," +
                " 'fields.submit_datetime.max'='1607356799000'" +
                ")";

        tableEnv.executeSql(dategen);
    }

    private static void registerFunction(StreamTableEnvironment tableEnv) {

//        tableEnv.createTemporarySystemFunction("RedisKey", new RedisKeyFunction());
//
//        RowTypeInfo rankingRowTypeInfoInteger = new RowTypeInfo(
//                new TypeInformation<?>[] {Types.STRING, Types.INT},
//                new String[] {"key", "value"});
//        RowTypeInfo rankingRowTypeInfoDecimal = new RowTypeInfo(
//                new TypeInformation<?>[] {Types.STRING, Types.BIG_DEC},
//                new String[] {"key", "value"});
//
//        tableEnv.registerFunction("Top10RankingInteger", new ArrayCollectFunction<Row>(rankingRowTypeInfoInteger, 10));
//        tableEnv.registerFunction("Top10RankingDecimal", new ArrayCollectFunction<Row>(rankingRowTypeInfoDecimal, 10));
//        tableEnv.registerFunction("Top50RankingDecimal", new ArrayCollectFunction<Row>(rankingRowTypeInfoDecimal, 50));
        tableEnv.registerFunction("SaleAmountMillionFunction", new SaleAmountMillionFunction(Types.STRING, 24 * 60));
//        tableEnv.createTemporarySystemFunction("RankingIntegerRedisValue",
//                new RedisValueFunction(Types.OBJECT_ARRAY(rankingRowTypeInfoInteger)));
//        tableEnv.createTemporarySystemFunction("RankingDecimalRedisValue",
//                new RedisValueFunction(Types.OBJECT_ARRAY(rankingRowTypeInfoDecimal)));

    }

    private static void loadConfig(StreamTableEnvironment tableEnv) throws IOException {
        // 获取配置，注册到全局
        ConfigFile configFile = ConfigService.getConfigFile("cube", ConfigFileFormat.Properties);
        cubeConfig = ParameterTool.fromPropertiesFile(new ByteArrayInputStream(configFile.getContent().getBytes()));

        // ConfigFile receptionConfigFile =
        // ConfigService.getConfigFile("cube-api-pc-reception", ConfigFileFormat.Properties);

        // UTF-8 乱码
        // receptionConfig = ParameterTool.fromPropertiesFile(
        // new ByteArrayInputStream(receptionConfigFile.getContent().getBytes("UTF-8")));

        // tableEnv.getConfig().addJobParameter(key, value);
        // tableEnv.getConfig().getConfiguration().set(PipelineOptions.GLOBAL_JOB_PARAMETERS,
        // Job Parameter Blink Plan Bug
        // Map<String, String> parameters = ParameterTool
        // .fromPropertiesFile(new ByteArrayInputStream(receptionConfig.getContent().getBytes()))
        // .toMap();
        // for (Map.Entry<String, String> entry : parameters.entrySet()) {
        // tableEnv.getConfig().addJobParameter(entry.getKey(), entry.getValue());
        // }
        // streamEnv.getConfig().setGlobalJobParameters(
        // ParameterTool.fromPropertiesFile(new ByteArrayInputStream(receptionConfig.getContent().getBytes())));

        // streamEnv.getConfig().setGlobalJobParameters(config);
    }

    private static String getKafkaProperties() {
        String kafkaProperties =
                String.format("'properties.bootstrap.servers' = '%s'", cubeConfig.get("cube.kafka.bootstrap.servers"));
        return kafkaProperties;
    }
}
