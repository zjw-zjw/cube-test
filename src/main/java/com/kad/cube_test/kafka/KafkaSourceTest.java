package com.kad.cube_test.kafka;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Properties;

import static org.apache.flink.table.api.Expressions.$;

/**
 *  读取Kafka数据
 */
public class KafkaSourceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.enableCheckpointing(60 * 60 * 1000);  // 1小时的checkpoint间隔
        streamEnv.setParallelism(1);

        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, bsSettings);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "cdh2.360kad.com:9092");
//        properties.setProperty("group.id", "test");
        properties.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "earliest");

        DataStream<String> order = streamEnv.addSource(new FlinkKafkaConsumer<>("ORACLE_OM", new SimpleStringSchema(), properties));
        DataStream<String> filterStream = order.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                JsonParser jsonParser = new JsonParser();
                JsonObject asJsonObject = jsonParser.parse(value).getAsJsonObject();
                if (asJsonObject.get("table").getAsString().equals("OM.OM_ORDERSTATUS")) {
                    String paymentdate = asJsonObject.get("after").getAsJsonObject().get("PAYMENTDATE").toString();
                    if (paymentdate.equals("null")) {
                        return false;
                    } else {
//                        System.out.println("------------>" + paymentdate);
//                        System.out.println(asJsonObject.get("after").getAsJsonObject().toString());
                        return true;
                    }
                } else {
                    return false;
                }
            }
        });
//        {"table":"OM.OM_ORDERSTATUS","op_type":"U","op_ts":"2020-10-14 11:10:03.997908","current_ts":"2020-10-14 11:10:08.708000","pos":"00000000320316892315","before":{"ORDERCODE":"1626140071","CUSCODE":"22382595381","ORDERTIME":"2020-10-14 10:02:12","ISCHECK":1,"CHECKER":"石翠竹","CHECKERCODE":"shicuizhu","CHECKDATE":"2020-10-14 10:57:19","ISCONSIGN":2,"CONSIGNDATE":"2020-10-14 10:58:31","ISOUT":0,"OUTDATE":null,"ISFINISH":0,"FINISHER":null,"FINISHERCODE":null,"FINISHDATE":null,"FINISHTYPECODE":null,"FINISHREASON":null,"ISCANCEL":0,"CANCELOR":null,"CANCELORCODE":null,"CANCELDATE":null,"CANCELTYPECODE":null,"CANCELREASON":null,"ISLOCK":5,"LOCKER":"系统","LOCKERCODE":"WMS","LOCKDATE":"2020-10-14 11:10:04","ISHIDING":0,"ISRXCHECK":0,"RXCHECKER":null,"RXCHECKERCODE":null,"RXCHECKDATE":null,"PAYMENTDATE":"2020-10-14 10:56:21","ORDERNUMBER":null,"RXCHECKSTATUS":null,"RXCHECKTEXT":null},"after":{"ORDERCODE":"1626140071","CUSCODE":"22382595381","ORDERTIME":"2020-10-14 10:02:12","ISCHECK":1,"CHECKER":"石翠竹","CHECKERCODE":"shicuizhu","CHECKDATE":"2020-10-14 10:57:19","ISCONSIGN":2,"CONSIGNDATE":"2020-10-14 10:58:31","ISOUT":2,"OUTDATE":"2020-10-14 11:10:04","ISFINISH":0,"FINISHER":null,"FINISHERCODE":null,"FINISHDATE":null,"FINISHTYPECODE":null,"FINISHREASON":null,"ISCANCEL":0,"CANCELOR":null,"CANCELORCODE":null,"CANCELDATE":null,"CANCELTYPECODE":null,"CANCELREASON":null,"ISLOCK":5,"LOCKER":"系统","LOCKERCODE":"WMS","LOCKDATE":"2020-10-14 11:10:04","ISHIDING":0,"ISRXCHECK":0,"RXCHECKER":null,"RXCHECKERCODE":null,"RXCHECKDATE":null,"PAYMENTDATE":"2020-10-14 10:56:21","ORDERNUMBER":4,"RXCHECKSTATUS":null,"RXCHECKTEXT":null}
        DataStream<Tuple2<String, String>> dataStream = filterStream.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String s) throws Exception {
                JsonParser jsonParser = new JsonParser();
                JsonObject asJsonObject = jsonParser.parse(s).getAsJsonObject();
                JsonElement after = asJsonObject.get("after");
                String ordercode = after.getAsJsonObject().get("ORDERCODE").toString();
                String paymentdate = after.getAsJsonObject().get("PAYMENTDATE").toString();
                return new Tuple2<>(ordercode, paymentdate);
            }
        }).returns(TypeInformation.of(new TypeHint<Tuple2<String, String>>() {}));

//        dataStream.print("after");

        tableEnv.createTemporaryView("order_pay_status", dataStream, $("ordercode"), $("paymentdate"));

        System.out.println(tableEnv.executeSql("select * from order_pay_status").getTableSchema());

        Table table = tableEnv.sqlQuery("select * from order_pay_status");
        tableEnv.toAppendStream(table, Row.class).print("status");

//        String jsonStr = "{\"table\":\"OM.OM_ORDERSTATUS\",\"op_type\":\"U\",\"op_ts\":\"2020-10-14 11:07:47.998870\",\"current_ts\":\"2020-10-14 11:07:52.424000\",\"pos\":\"00000000320316607609\",\"before\":{\"ORDERCODE\":\"1601493831\",\"CUSCODE\":null,\"ORDERTIME\":null,\"ISCHECK\":null,\"CHECKER\":null,\"CHECKERCODE\":null,\"CHECKDATE\":null,\"ISCONSIGN\":null,\"CONSIGNDATE\":null,\"ISOUT\":null,\"OUTDATE\":null,\"ISFINISH\":null,\"FINISHER\":null,\"FINISHERCODE\":null,\"FINISHDATE\":null,\"FINISHTYPECODE\":null,\"FINISHREASON\":null,\"ISCANCEL\":null,\"CANCELOR\":null,\"CANCELORCODE\":null,\"CANCELDATE\":null,\"CANCELTYPECODE\":null,\"CANCELREASON\":null,\"ISLOCK\":0,\"LOCKER\":null,\"LOCKERCODE\":null,\"LOCKDATE\":null,\"ISHIDING\":null,\"ISRXCHECK\":null,\"RXCHECKER\":null,\"RXCHECKERCODE\":null,\"RXCHECKDATE\":null,\"PAYMENTDATE\":null,\"ORDERNUMBER\":null,\"RXCHECKSTATUS\":null,\"RXCHECKTEXT\":null},\"after\":{\"ORDERCODE\":\"1601493831\",\"CUSCODE\":null,\"ORDERTIME\":null,\"ISCHECK\":null,\"CHECKER\":null,\"CHECKERCODE\":null,\"CHECKDATE\":null,\"ISCONSIGN\":null,\"CONSIGNDATE\":null,\"ISOUT\":null,\"OUTDATE\":null,\"ISFINISH\":null,\"FINISHER\":null,\"FINISHERCODE\":null,\"FINISHDATE\":null,\"FINISHTYPECODE\":null,\"FINISHREASON\":null,\"ISCANCEL\":null,\"CANCELOR\":null,\"CANCELORCODE\":null,\"CANCELDATE\":null,\"CANCELTYPECODE\":null,\"CANCELREASON\":null,\"ISLOCK\":11,\"LOCKER\":\"仓库作业\",\"LOCKERCODE\":\"仓库作业\",\"LOCKDATE\":\"2020-10-14 11:07:48\",\"ISHIDING\":null,\"ISRXCHECK\":null,\"RXCHECKER\":null,\"RXCHECKERCODE\":null,\"RXCHECKDATE\":null,\"PAYMENTDATE\":null,\"ORDERNUMBER\":null,\"RXCHECKSTATUS\":null,\"RXCHECKTEXT\":null}}";
//        JsonParser jsonParser = new JsonParser();
//        JsonObject asJsonObject = jsonParser.parse(jsonStr).getAsJsonObject();
//        System.out.println(asJsonObject.get("after").getAsJsonObject().toString());
        streamEnv.execute("test job");
    }
}
