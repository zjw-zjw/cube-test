package com.kad.cube_test.dwd;

import com.ctrip.framework.apollo.ConfigFile;
import com.ctrip.framework.apollo.ConfigService;
import com.ctrip.framework.apollo.core.enums.ConfigFileFormat;
import com.kad.cube_test.kudu.DimDateTimeFunction;
import com.kad.cube_test.kudu.OfflineBatchToKudu;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.catalog.JdbcCatalog;
import org.apache.flink.connectors.kudu.table.KuduCatalog;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.types.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

/**
 *  离线  DWD层 零售订单支付事实表
 */
public class DwdOrderRetailOrderPay {
    private static ParameterTool config;
    private static Logger LOG = LoggerFactory.getLogger(DwdOrderRetailOrderPay.class);
    public static void main(String[] args) throws IOException, TableNotExistException {
        // 创建处理环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);

        // 创建表执行环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);
        Configuration configuration = tableEnv.getConfig().getConfiguration();
        configuration.setBoolean("table.dynamic-table-options.enabled", true);
        configuration.setLong("taskmanager.memory.task.heap.size", 8388608);


//        18:15:42,397 INFO  org.apache.flink.runtime.taskexecutor.TaskExecutorResourceUtils [] - The configuration option Key: 'taskmanager.cpu.cores' , default: null (fallback keys: []) required for local execution is not set, setting it to its default value 1.7976931348623157E308
//        18:15:42,398 INFO  org.apache.flink.runtime.taskexecutor.TaskExecutorResourceUtils [] - The configuration option Key: 'taskmanager.memory.task.heap.size' , default: null (fallback keys: []) required for local execution is not set, setting it to its default value 9223372036854775807 bytes
//        18:15:42,398 INFO  org.apache.flink.runtime.taskexecutor.TaskExecutorResourceUtils [] - The configuration option Key: 'taskmanager.memory.task.off-heap.size' , default: 0 bytes (fallback keys: []) required for local execution is not set, setting it to its default value 9223372036854775807 bytes
//        18:15:42,398 INFO  org.apache.flink.runtime.taskexecutor.TaskExecutorResourceUtils [] - The configuration option Key: 'taskmanager.memory.network.min' , default: 64 mb (fallback keys: [{key=taskmanager.network.memory.min, isDeprecated=true}]) required for local execution is not set, setting it to its default value 64 mb
//        18:15:42,398 INFO  org.apache.flink.runtime.taskexecutor.TaskExecutorResourceUtils [] - The configuration option Key: 'taskmanager.memory.network.max' , default: 1 gb (fallback keys: [{key=taskmanager.network.memory.max, isDeprecated=true}]) required for local execution is not set, setting it to its default value 64 mb
//        18:15:42,398 INFO  org.apache.flink.runtime.taskexecutor.TaskExecutorResourceUtils [] - The configuration option Key: 'taskmanager.memory.managed.size' , default: null (fallback keys: [{key=taskmanager.memory.size, isDeprecated=true}]) required for local execution is not set, setting it to its default value 128 mb

        loadConfig(env);                // 加载全局配置
        initHiveCatalog(tableEnv);      // 初始化 Hive Catalog
        initMysqlCatalog(tableEnv);     // 初始化 Mysql Catalog
        initKuduCatalog(tableEnv);      // 初始化 Kudu Catalog
        initOrderDimDateTimeFunction(tableEnv);

        initRetailPayFact(tableEnv);
        sinkDwdRetailPay(tableEnv);
    }


    private static void sinkDwdRetailPay(TableEnvironment tableEnv) throws TableNotExistException {
        /**
         *  与 维度表 join 形成 DWD表
         */
        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        String options ="/*+ OPTIONS('lookup.cache.ttl'='86400', 'lookup.cache.max-rows'='10000') */";
        String querySql = "SELECT * "
                + "FROM "
                + " fact_order_retail_pay_offline as retail_order_pay "
                + " LEFT JOIN LATERAL TABLE(PAY_DIM_DATE_TIME(cast(retail_order_pay.pay_datetime as TIMESTAMP))) as pay_datetime ON true "
                + " LEFT JOIN `mysql`.`cube`.`dim_address` " + options
                + " FOR SYSTEM_TIME AS OF retail_order_pay.proctime AS dim_address ON dim_address.address_id = retail_order_pay.address_id "
                + " LEFT JOIN `mysql`.`cube`.`dim_ware` " + options
                + " FOR SYSTEM_TIME AS OF retail_order_pay.proctime AS dim_ware ON dim_ware.ware_id = retail_order_pay.ware_id ";
        Table dwdTable = tableEnv.sqlQuery(querySql);
        dwdTable.printSchema();
        tableEnv.createTemporaryView("dwd_order_retail_pay", dwdTable);

        String[] castFieldsArray = convertCastFields(tableEnv, "kudu", "impala_kudu", "dwd_order_retail_order_pay_test");
        String castFields = String.join(",", castFieldsArray);
        System.out.println(castFields);

        String upsertToKuduSql = "UPSERT INTO `kudu`.`default_database`.`impala::impala_kudu.dwd_order_retail_order_pay_test` SELECT "
                + castFields
                + " FROM "
                + " dwd_order_retail_pay";
        StatementSet statementSet = tableEnv.createStatementSet();
        statementSet.addInsertSql(upsertToKuduSql);
        statementSet.execute();
    }
//        2020-10-27
//    2	2020-06-09
//    3	2020-11-01
//    4	2020-11-02
//    5	2020-10-31
//    6	2020-06-10
    private static void initRetailPayFact(TableEnvironment tableEnv) {

        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        String temp = " SELECT \n" +
                "        date_format(ordertime, 'yyyy-MM-dd') as p_day \n" +
                "    FROM \n" +
                "        hive.myhive.ods_om_om_orderstatus \n" +
                "    WHERE \n" +
                "       paymentdate <> '' and paymentdate is not null " +
                "    and    " +
                "       date_format(paymentdate, 'yyyy-MM-dd') between '2020-11-01' and '2020-11-02'\n" +
                "    GROUP BY \n" +
                "        date_format(ordertime, 'yyyy-MM-dd')";
//        Table tempTable = tableEnv.sqlQuery(temp);
//        tableEnv.createTemporaryView("temp", tempTable);

        /**
         *  选取的订单状态表数据，与 订单详情表、订单表、商品表、订单地址表等 join 形成 FACT表
         */
        String orderStatusSourceSql = "select \n" +
                "            ordercode,\n" +
                "            paymentdate\n" +
                "        from \n" +
                "            hive.myhive.ods_om_om_orderstatus \n" +
                "        where \n" +
                "            paymentdate <> '' and paymentdate is not null\n" +
                "        and \n" +
                "            date_format(paymentdate, 'yyyy-MM-dd') between '2020-11-01' and '2020-11-02'";

        // 订单详情表
        String orderDetailSourceSql = "select " +
                " detailid, " +
                " ordercode, " +
                " warecode, " +
                " ordertime," +
                " qty, " +
                " sumnetamt " +
                " from " +
                "   `hive`.`myhive`.ods_om_om_orderdetail as t" +
//                " left semi join " +
//                "  temp as b" +
//                " on t.p_day = b.p_day ";
                "  where p_day in (select * from temp as a)";
//                " where p_day in ('2020-10-27', '2020-06-09', '2020-11-01', '2020-11-02', '2020-10-31', '2020-06-10')";
        // 订单表
        String orderSourceSql = "select " +
                " `ordercode`,"      +        // 订单ID
                " `cuscode`,"        +        // 客户ID
                " `changetype`,"     +        // 换货类型 -> 订单类型
                " `ordersource`,"    +        // 订单来源
                " `shopcode`,"       +        // 下单门店编号或下单第三方店铺编号
                " `salemode`,"       +        // 销售模式
                " `invoicetype`,"    +        // 登记发票类型
                " `createdate` "     +        // 下单时间
                "  from " +
                "    `hive`.`myhive`.ods_om_om_order as t" +
//                " left semi join " +
//                "   temp as b" +
//                " on t.p_day = b.p_day";
//                "  where p_day in ('2020-10-27', '2020-06-09', '2020-11-01', '2020-11-02', '2020-10-31', '2020-06-10') " +
                " where p_day in (select * from temp as a)  ";
//                " and `changetype` not in (1, 2, 3)"; // -- 过滤掉换货订单：1-普通换货单 2-重发订单 3-先发后退换货单
        // 订单地址表
        String orderAddressSql = "select " +
                " `ordercode`,"     +        // 订单ID
                " `provincecode`,"  +        // 省编号
                " `citycode`,"      +        // 市编号
                " `areacode` "      +        // 地址编号，最后一级地址的编号
                " from " +
                " `hive`.`myhive`.ods_om_om_orderaddress as t" +
//                " left semi join " +
//                "   temp as b" +
//                " on t.p_day = b.p_day";
//                "  where p_day in ('2020-10-27', '2020-06-09', '2020-11-01', '2020-11-02', '2020-10-31', '2020-06-10')";
                " where p_day in (select * from temp as a)";

        String createFactSql =
                " WITH temp as (" + temp +") "+
                " SELECT " +
                        " od.`detailid` as id," +
                        " o.`cuscode` as customer_id, " +
                        " od.`warecode` as ware_id," +
                        " CONCAT_WS('-', oa.`provincecode`, oa.`citycode`) as address_id," +
                        " os.`ordercode` as order_id," +
                        " os.`paymentdate` as pay_datetime," +
                        " o.`createdate` as order_datetime," +
                        " od.`qty` as pay_count," +
                        " od.`sumnetamt` as pay_amount," +
                        // -- 订单维表属性
                        " o.`ordersource` as order_source_id," +
                        " o.`shopcode` as order_shop_id," +
                        " CONCAT_WS('-', CAST(o.`ordersource` AS STRING), o.`shopcode`) as order_sale_channel_id," +
                        " o.`changetype` as order_type," +
                        " o.`salemode` as order_sale_mode," +
                        " o.`invoicetype` as order_invoice_type, " +
                        " PROCTIME() as proctime " +
                        " FROM " +
                        " ( " + orderStatusSourceSql + " ) as os " +
                        " join " +
                        " ( " + orderDetailSourceSql + " ) as od " +
                        " on os.ordercode = od.ordercode " +
                        " join " +
                        " ( " + orderSourceSql + " ) as o " +
                        " on os.ordercode = o.ordercode " +
                        " join " +
                        " ( "+ orderAddressSql + " ) as oa " +
                        " on os.ordercode = oa.ordercode " +
                        " where o.changetype not in (1, 2, 3)";

//        tableEnv.executeSql("CREATE TEMPORARY VIEW `default_catalog`.`default_database`.`om_order_detail` AS " + orderDetailSourceSql);
//        tableEnv.executeSql("CREATE TEMPORARY VIEW `default_catalog`.`default_database`.`om_order` AS " + orderSourceSql);
//        tableEnv.executeSql("CREATE TEMPORARY VIEW `default_catalog`.`default_database`.`om_order_address` AS " + orderAddressSql);
        System.out.println(createFactSql);
        Table table = tableEnv.sqlQuery(createFactSql);
        tableEnv.createTemporaryView("fact_order_retail_pay_offline", table);
    }

    private static String[] convertCastFields(TableEnvironment tableEnv, String catalog, String database, String tableName) throws TableNotExistException {
        CatalogBaseTable table = tableEnv.getCatalog(catalog).get().getTable(new ObjectPath("default_database", "impala::"+database+"."+tableName));
        TableSchema schema = table.getSchema();
        String[] fieldNames = schema.getFieldNames();
        DataType[] fieldDataTypes = schema.getFieldDataTypes();

        ArrayList<String> castFieldList = new ArrayList<>();
        for (String fieldName : fieldNames) {
            if ( !fieldName.equals("p_day") ) {
                for (DataType fieldDataType : fieldDataTypes) {
                    if(schema.getFieldDataType(fieldName).get().toString().equals(fieldDataType.toString())) {
                        String typeStr = fieldDataType.toString();
                        if (typeStr.contains("NOT NULL")){
                            String type = typeStr.split(" ")[0];
                            String castFieldStr = "CAST(" + fieldName + " AS " + type + ") AS " + fieldName;
                            castFieldList.add(castFieldStr);
                        } else {
                            String castFieldStr = "CAST(" + fieldName + " AS " + typeStr + ") AS " + fieldName;
                            castFieldList.add(castFieldStr);
                        }
                        break;
                    }
                }
            }
        }
        return castFieldList.toArray(new String[castFieldList.size()]);
    }

    private static void initOrderDimDateTimeFunction(TableEnvironment tableEnv) {
        tableEnv.createTemporarySystemFunction("ORDER_DIM_DATE_TIME", new DimDateTimeFunction("submit"));     // 下单日期维度
        tableEnv.createTemporarySystemFunction("PAY_DIM_DATE_TIME", new DimDateTimeFunction("pay"));          // 支付日期维度
    }


    private static void loadConfig(StreamExecutionEnvironment streamEnv) throws IOException {
        // 获取配置，注册到全局
        ConfigFile configFile = ConfigService.getConfigFile("cube", ConfigFileFormat.Properties);
        config = ParameterTool.fromPropertiesFile(new ByteArrayInputStream(configFile.getContent().getBytes()));
        streamEnv.getConfig().setGlobalJobParameters(config);
    }

    private static void initMysqlCatalog(TableEnvironment tableEnv) {
        String name = "mysql";
        String defaultDatabase = "cube";
        String username = config.get("cube.mysql.username");
        String password = config.get("cube.mysql.password");
        String baseUrl = config.get("cube.mysql.jdbcUrl").split("\\?")[0];
        JdbcCatalog catalog = new JdbcCatalog(name, defaultDatabase, username, password, baseUrl);
        tableEnv.registerCatalog("mysql", catalog);
    }

    private static void initPhoenixCatalog(TableEnvironment tableEnv) {
        String catalog = "phoenix";
        String defaultDatabase = "cube_phx";
        String username = config.get("cube.phoenix.username");
        String password = config.get("cube.phoenix.password");
//        String baseUrl =  config.get("cube.phoenix.jdbcUrl");
        String baseUrl = "jdbc:phoenix:cdh2.360kad.com,cdh4.360kad.com,cdh5.360kad.com:2181;autoCommit=true";
        JdbcCatalog phoenixCatalog = new JdbcCatalog(catalog, defaultDatabase, username, password, baseUrl);
        tableEnv.registerCatalog("phoenix", phoenixCatalog);
    }

    private static void initKuduCatalog(TableEnvironment tableEnv) {
        String KUDU_MASTERS = config.get("cube.kudu.masterAddresses");
        KuduCatalog catalog = new KuduCatalog(KUDU_MASTERS);
        tableEnv.registerCatalog("kudu", catalog);
    }

    private static void initHiveCatalog(TableEnvironment tableEnv) {
        String name = "hive";
        String defaultDatabase = "myhive";
        String hiveConfDir = "src/main/resources"; // a local path
        String version = "2.1.1";
        HiveCatalog hiveCatalog = new HiveCatalog(name, defaultDatabase, hiveConfDir, version);
        tableEnv.registerCatalog("hive", hiveCatalog);
    }
}
