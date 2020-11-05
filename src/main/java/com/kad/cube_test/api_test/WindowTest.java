package com.kad.cube_test.api_test;


import com.kad.cube_test.model.SensorReading;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 *  table api的窗口操作
 */
public class WindowTest {
    public static void main(String[] args) throws Exception {
        // 创建流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 设置时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 创建表执行环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // 获取输入流
        DataStream<String> sourceStream = env.readTextFile("D:\\develop\\kadProjects\\cube_test\\src\\data\\sensor.txt");
        // 转换成pojo类
        DataStream<SensorReading> dataStream = sourceStream.map(data -> {
                    String[] dataArray = data.split(",");
                    return new SensorReading(dataArray[0], Long.valueOf(dataArray[1]), Double.valueOf(dataArray[2]));
                }
        ).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(1)) {
            @Override
            public long extractTimestamp(SensorReading element) {
                return element.getTime() * 1000L;
            }
        });


//        dataStream.print();

        // 将dataStream转换成 Table
        Table sensorTable = tableEnv.fromDataStream(dataStream);

        tableEnv.createTemporaryView("sensor", sensorTable);

        Table resultTable = tableEnv.sqlQuery(
                "select id, temperature " +
                        "from (" +
                        "select *, ROW_NUMBER() OVER(PARTITION BY id ORDER BY temperature desc) as row_num from sensor" +
                        ") " +
                        "WHERE row_num <= 2"
        );

        tableEnv.toRetractStream(resultTable, Row.class).print("result");

        env.execute("job");
    }
}
