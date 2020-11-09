package com.kad.cube_test.hive;

import com.kad.cube_test.model.OmOrder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.hive.HiveCatalog;

public class ReadOracleSinkToHive {
    static StreamExecutionEnvironment streamEnv;
    static StreamTableEnvironment tableEnv;
    public static void main(String[] args) throws Exception {
        initTableEnvironment();
        initHiveCatalog(tableEnv);

        DataStream<OmOrder> dataStream = readOracleSource(streamEnv);
        dataStream.print("order");

        Table table = tableEnv.fromDataStream(dataStream);
        table.printSchema();
        tableEnv.createTemporaryView("om_order", table);
//        tableEnv.toAppendStream(tableEnv.sqlQuery("select * from om_order"),Row.class).print("om_order");

//        TableSchema schema = table.getSchema();
//        DataType dataType = schema.toPhysicalRowDataType();
//        LogicalType logicalType = dataType.getLogicalType();
//        System.out.println("Physical Type: " + dataType.toString());
//        System.out.println("Logical Type: " + logicalType.toString());

        TableSchema schema = tableEnv.getCatalog("myhive").get().getTable(new ObjectPath("test", "ods_om_om_order_test")).getSchema();
        System.out.println("hive - ods_om_om_order_test: \n" + schema.toString());

//        sinkToHive(tableEnv);

        streamEnv.execute("test oracle source job");
    }

    private static void sinkToHive(StreamTableEnvironment tableEnv) throws TableNotExistException {
        String[] fieldNames = tableEnv.getCatalog("myhive").get().getTable(new ObjectPath("test", "ods_om_om_order_test")).getSchema().getFieldNames();
        String fileds = String.join(",", fieldNames);
        String sql = "insert into `myhive`.test.ods_om_om_order_test select " + fileds + " from om_order";
        tableEnv.executeSql(sql);
    }


    private static DataStream<OmOrder> readOracleSource(StreamExecutionEnvironment streamEnv) {
        DataStreamSource<OmOrder> source = streamEnv.addSource(new OmOrderSourceFunction<OmOrder>());
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
