package com.kad.cube_test.hive;

import com.kad.cube_test.model.Type;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

import java.math.BigDecimal;
import java.math.BigInteger;

public class HiveTypeTest {
    static StreamExecutionEnvironment streamEnv;
    static StreamTableEnvironment tableEnv;

    public static void main(String[] args) throws Exception {
        initTableEnvironment();
        initHiveCatalog(tableEnv);

        DataStream<Type> dataStream = streamEnv.fromElements(
                new Type("aaa", 12345, 4324L, Short.valueOf("1342"), Byte.valueOf("23"), BigDecimal.valueOf(43.22), BigDecimal.valueOf(242343.34545), 34.434d, 2342.34f)
        );
        dataStream.print("type");

        tableEnv.createTemporaryView("type_test", dataStream, "_varchar, _int, bigint, smallint, tinyint, decimal_5_4, decimal_18_4, _double, _float");
        tableEnv.fromDataStream(dataStream, "_varchar, _int, bigint, smallint, tinyint, decimal_5_4, decimal_18_4, _double, _float").printSchema();
        tableEnv.executeSql(
                "insert into " +
                        "`myhive`.`test`.type_test " +
                        "select " +
                        "   `_varchar`, `_int`, `bigint`, `smallint`, `tinyint`, `decimal_5_4`, `decimal_18_4`, `_double`, `_float`" +
                        " from type_test"
        );

        streamEnv.execute("type test job");
//        Table table = tableEnv.sqlQuery("select " +
//                "   `_varchar`, `_int`, `bigint`, `smallint`, `tinyint`, `decimal_5_4`, `decimal_18_4`, `_double`, `_float`" +
//                " from type_test");
//        table.insertInto("`myhive`.`test`.type_test");
    }

    private static void initTableEnvironment() {
        streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(1);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        tableEnv = StreamTableEnvironment.create(streamEnv, settings);
    }

    private static void initHiveCatalog(StreamTableEnvironment tableEnv) {
        String name = "myhive";
        String defaultDatabase = "default";
        String hiveConfDir = "src/main/resources"; // a local path
        String version = "2.1.1";
        HiveCatalog hiveCatalog = new HiveCatalog(name, defaultDatabase, hiveConfDir, version);
        tableEnv.registerCatalog("myhive", hiveCatalog);
    }
}
