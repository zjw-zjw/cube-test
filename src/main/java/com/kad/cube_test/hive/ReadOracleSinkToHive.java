package com.kad.cube_test.hive;

import com.kad.cube_test.model.OmOrder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import javax.xml.crypto.Data;
import java.util.Map;

public class ReadOracleSinkToHive {
    static StreamExecutionEnvironment streamEnv;
    static StreamTableEnvironment tableEnv;
    public static void main(String[] args) throws Exception {
        initTableEnvironment();
        initHiveCatalog(tableEnv);


        DataStream<OmOrder> dataStream = readOracleSource(streamEnv);
        dataStream.print("order");
        tableEnv.createTemporaryView("order", dataStream);
//        typeSchemaInfoTest(tableEnv, dataStream);

//        Table table = tableEnv.fromDataStream(dataStream);
//        tableEnv.createTemporaryView("om_order", table);

//        TableSchema schema = tableEnv.getCatalog("myhive").get().getTable(new ObjectPath("test", "ods_om_om_order_test")).getSchema();
//        System.out.println("hive - ods_om_om_order_test: \n" + schema.toString());

//        sinkToHive(tableEnv);

        streamEnv.execute("test oracle source job");
    }

    private static void typeSchemaInfoTest(StreamTableEnvironment tableEnv, DataStream<OmOrder> dataStram) throws TableNotExistException {
        CatalogBaseTable table = tableEnv.getCatalog("myhive").get().getTable(new ObjectPath("test", "ods_om_om_order_test"));
        TableSchema schema = table.getSchema();
        DataType produceDataType = schema.toPhysicalRowDataType();
        System.out.println(produceDataType.toString());

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
