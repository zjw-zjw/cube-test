package com.kad.cube_test.api_test;

import com.kad.cube_test.model.PlayerData;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;


/**
 *  flink catalog
 *
 */
public class CatalogTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointInterval(60 * 60 * 1000);
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.setParallelism(1);

        EnvironmentSettings bsSetiings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, bsSetiings);

//        String
//        tableEnv.executeSql();

        RowTypeInfo rankingRowTypeInfoInteger = new RowTypeInfo(
                new TypeInformation<?>[] {Types.STRING, Types.INT},
                new String[] {"key", "value"});
        RowTypeInfo rankingRowTypeInfoDecimal = new RowTypeInfo(
                new TypeInformation<?>[] {Types.STRING, Types.BIG_DEC},
                new String[] {"key", "value"});

        TypeInformation<Row[]> typeInformation = Types.OBJECT_ARRAY(rankingRowTypeInfoInteger);
        System.out.println(typeInformation.toString());
    }
}
