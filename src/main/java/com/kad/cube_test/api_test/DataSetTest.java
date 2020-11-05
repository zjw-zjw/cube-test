package com.kad.cube_test.api_test;

import com.kad.cube_test.model.PlayerData;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.ReduceOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;

import javax.xml.crypto.Data;
import java.util.concurrent.ExecutionException;

public class DataSetTest {
    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<PlayerData> playerDataDataSet = env.readCsvFile("D:\\develop\\kadProjects\\cube_test\\src\\data\\score.csv")
                .pojoType(PlayerData.class, "season", "player", "play_num", "first_court", "time", "assists", "steals", "blocks", "scores");

      DataSet<Tuple2<String, Integer>> resultDataset = playerDataDataSet
                .filter(new MyFilterFunction())
                .map(new MyMapFunction())
                .groupBy(0)
                .sum(1);

        resultDataset.print("");


        env.execute("");
    }

    public static class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>>{
        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            for (String word: s.split(" ")) {
                collector.collect(new Tuple2<>(word, 1));
            }
        }
    }

    private static class MyFilterFunction implements org.apache.flink.api.common.functions.FilterFunction<PlayerData> {
        @Override
        public boolean filter(PlayerData playerData) throws Exception {
            return playerData.player.equals("迈克尔-乔丹");
        }
    }


    private static class MyReduceFunction implements org.apache.flink.api.common.functions.ReduceFunction<PlayerData> {
        @Override
        public PlayerData reduce(PlayerData playerData, PlayerData t1) throws Exception {
            t1.play_num = playerData.play_num + t1.play_num;
            return t1;
        }
    }

    private static class MyMapFunction implements org.apache.flink.api.common.functions.MapFunction<PlayerData, Tuple2<String, Integer>> {
        @Override
        public Tuple2<String, Integer> map(PlayerData playerData) throws Exception {
            return new Tuple2<>(playerData.player, 1);
        }
    }
}
