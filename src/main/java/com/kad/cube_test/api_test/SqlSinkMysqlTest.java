package com.kad.cube_test.api_test;

import com.kad.cube_test.model.PlayerData;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;

public class SqlSinkMysqlTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointInterval(60 * 60 * 1000);
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        env.setParallelism(1);

        EnvironmentSettings bsSetiings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, bsSetiings);

        // enable checkpoint
        tableEnv.getConfig().getConfiguration().set(ExecutionCheckpointingOptions.CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE);
        tableEnv.getConfig().getConfiguration().set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(10));

        DataStream<String> dataSource = env.readTextFile("D:\\develop\\kadProjects\\cube_test\\src\\data\\score.csv");

        // 转换成pojo
        DataStream<PlayerData> dataStream = dataSource.map(line -> {
            String[] split = line.split(",");
            return new PlayerData(
                    String.valueOf(split[0]),
                    String.valueOf(split[1]),
                    String.valueOf(split[2]),
                    Integer.valueOf(split[3]),
                    Double.valueOf(split[4]),
                    Double.valueOf(split[5]),
                    Double.valueOf(split[6]),
                    Double.valueOf(split[7]),
                    Double.valueOf(split[8])
            );
        });

        // register the DataStream as View "playerScore"   创建视图
        tableEnv.createTemporaryView("playerScore", dataStream);

        // mysql的 DDL语句
        String DDL = "CREATE TABLE playerScoreSink (\n" +
                "  `season` STRING COMMENT '赛季',\n" +
                "  `player` STRING COMMENT '运动员',\n" +
                "  `play_num` STRING,\n" +
                "  `first_court` INT,\n" +
                "  `time` DOUBLE,\n" +
                "  `assists` DOUBLE,\n" +
                "  `steals` DOUBLE,\n" +
                "  `blocks` DOUBLE,\n" +
                "  `scores` DOUBLE\n" +
                ") WITH ( " +
                    "'connector' = 'jdbc',\n" +
                    "'url' = 'jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=utf8',\n" +
                    "'table-name' = 'playerscore', \n" +
                    "'username' = 'root', \n" +
                    "'password' = '123456' \n" +
                ")";

        // 创建mysql Table
        tableEnv.executeSql(DDL);
        // 查询并输出
        Table query = tableEnv.sqlQuery("select * from playerScoreSink");
        tableEnv.toAppendStream(query, Row.class).print("mysql table");

        // 核心逻辑处理SQL编写： 这里注意 字段里有一个 time字段，与关键字冲突，需要加上``号
        String sql = "select " +
                " season, player, play_num, first_court, `time`, assists, steals, blocks, scores" +
                " from playerScore where player = '迈克尔-乔丹'";
        // 执行SQL
        Table resultTable = tableEnv.sqlQuery(sql);

//        resultTable.toString();

        tableEnv.createTemporaryView("filter_result", resultTable);
        tableEnv.toAppendStream(resultTable, Row.class).print("filter_result");

        // sink 到 mysql表中
//        tableEnv.executeSql("insert into playerScoreSink select * from filter_result");

        env.execute("table api job test");
    }
}
