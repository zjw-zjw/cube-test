package com.kad.cube_test.hive;

import com.kad.cube_test.model.OmOrder;
import org.apache.flink.connector.jdbc.table.JdbcTableSource;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.types.Row;

public class ReadOracleSinkToHive {
    static StreamExecutionEnvironment streamEnv;
    static StreamTableEnvironment tableEnv;
    public static void main(String[] args) throws Exception {
        initTableEnvironment();
        initHiveCatalog(tableEnv);

        DataStream<Row> dataStream = readOracleSource(streamEnv);
//        tableEnv.createTemporaryView("aaa", dataStream);
        dataStream.print("order");

        TableSchema schema = tableEnv.getCatalog("myhive").get().getTable(new ObjectPath("test", "ods_om_om_order_test")).getSchema();
        System.out.println("hive - ods_om_om_order_test: \n" + schema.toString());

//        sinkToHive(tableEnv);

        streamEnv.execute("test oracle source job");
    }

    private static void sinkToHive(StreamTableEnvironment tableEnv) throws TableNotExistException {
        String[] fieldNames = tableEnv.getCatalog("myhive").get().getTable(new ObjectPath("test", "ods_om_om_order_test")).getSchema().getFieldNames();
        String fileds = String.join(",", fieldNames);
        String sql = "insert into `myhive`.test.ods_om_om_order_test select "+ fileds +" from om_order";
        tableEnv.executeSql(sql);
    }

    private static void sinkCsvFile(StreamExecutionEnvironment streamEnv, DataStream<OmOrder> dataStream) {

    }


    private static DataStream<Row> readOracleSource(StreamExecutionEnvironment streamEnv) {
        DataStreamSource<Row> source = streamEnv.addSource(new OmOrderSourceFunction<Row>());
        return source;
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
