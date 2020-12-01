package com.kad.cube_test.kudu;

import com.ctrip.framework.apollo.ConfigFile;
import com.ctrip.framework.apollo.ConfigService;
import com.ctrip.framework.apollo.core.enums.ConfigFileFormat;
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

public class OfflineBatchToKudu {
    private static ParameterTool config;
    private static Logger LOG = LoggerFactory.getLogger(OfflineBatchToKudu.class);
    public static void main(String[] args) throws Exception {
        // 创建处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 创建表执行环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);
        Configuration configuration = tableEnv.getConfig().getConfiguration();
        configuration.setBoolean("table.dynamic-table-options.enabled", true);

        loadConfig(env);                // 加载全局配置
        initHiveCatalog(tableEnv);      // 初始化 Hive Catalog
        initPhoenixCatalog(tableEnv);   // 初始化 Phoenix Catalog
        initMysqlCatalog(tableEnv);     // 初始化 Mysql Catalog
        initKuduCatalog(tableEnv);      // 初始化 Kudu Catalog

        /**
         *  选取的订单详情表数据，与 订单表、商品表、订单地址表等 join 形成 FACT表
         */
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        // 订单详情表
        String orderDetailSourceSql = "select " +
                " detailid, " +
                " ordercode, " +
                " warecode, " +
                " ordertime," +
                " qty, " +
                " sumnetamt, " +
                " date_format(ordertime, 'yyyy-MM-dd') as p_day " +
                " from " +
                " `hive`.`myhive`.ods_om_om_orderdetail " +
                "  where p_day < '2020-07-12' and p_day >= '2020-05-12'";
        // 订单表
        String orderSourceSql = "select " +
                " `ordercode`,"      +        // 订单ID
                " `cuscode`,"        +        // 客户ID
                " `changetype`,"     +        // 换货类型 -> 订单类型
                " `ordersource`,"    +        // 订单来源
                " `shopcode`,"       +        // 下单门店编号或下单第三方店铺编号
                " `salemode`,"       +        // 销售模式
                " `invoicetype`,"    +        // 登记发票类型
                " `createdate`, "     +        // 下单时间
                " date_format(createdate, 'yyyy-MM-dd') as p_day " +
                "  from " +
                " `hive`.`myhive`.ods_om_om_order " +
                "  where p_day < '2020-07-12' and p_day >= '2020-05-12' and `changetype` not in (1, 2, 3)"; // -- 过滤掉换货订单：1-普通换货单 2-重发订单 3-先发后退换货单
        // 订单地址表
        String orderAddressSql = "select " +
                " `ordercode`,"     +        // 订单ID
                " `provincecode`,"  +        // 省编号
                " `citycode`,"      +        // 市编号
                " `areacode`, "      +        // 地址编号，最后一级地址的编号
                " date_format(ordertime, 'yyyy-MM-dd') as p_day " +
                " from " +
                " `hive`.`myhive`.ods_om_om_orderaddress " +
                "  where p_day < '2020-07-12' and p_day >= '2020-05-12'";

        String createFactSql =
                " SELECT " +
                " od.`detailid` as id," +
                " o.`cuscode` as customer_id, " +
                " od.`warecode` as ware_id," +
                " CONCAT_WS('-', oa.`provincecode`, oa.`citycode`) as address_id," +
                " o.`ordercode` as order_id," +
                " od.`ordertime` as submit_datetime," +
                " o.`createdate` as order_datetime," +
                " od.`qty` as submit_count," +
                " od.`sumnetamt` as submit_amount," +
                // -- 订单维表属性
                " o.`ordersource` as order_source_id," +
                " o.`shopcode` as order_shop_id," +
                " CONCAT_WS('-', CAST(o.`ordersource` AS STRING), o.`shopcode`) as order_sale_channel_id," +
                " o.`changetype` as order_type," +
                " o.`salemode` as order_sale_mode," +
                " o.`invoicetype` as order_invoice_type, " +
                " PROCTIME() as proctime " +
                " FROM " +
                "   ( " + orderDetailSourceSql + " ) as od " +
                " join " +
                "   ( " + orderSourceSql + " ) as o " +
                " on od.ordercode = o.ordercode and od.p_day = o.p_day" +
                " join " +
                "    ( "+ orderAddressSql + " ) as oa " +
                " on od.ordercode = oa.ordercode and od.p_day = oa.p_day";

//        tableEnv.executeSql("CREATE TEMPORARY VIEW `default_catalog`.`default_database`.`om_order_detail` AS " + orderDetailSourceSql);
//        tableEnv.executeSql("CREATE TEMPORARY VIEW `default_catalog`.`default_database`.`om_order` AS " + orderSourceSql);
//        tableEnv.executeSql("CREATE TEMPORARY VIEW `default_catalog`.`default_database`.`om_order_address` AS " + orderAddressSql);
        Table table = tableEnv.sqlQuery(createFactSql);
        tableEnv.createTemporaryView("fact_order_retail_offline", table);


        /**
         *  与 维度表 join 形成 DWD表
         */
        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        initOrderDimDateTimeFunction(tableEnv);
        String options ="/*+ OPTIONS('lookup.cache.ttl'='86400', 'lookup.cache.max-rows'='10000') */";
        String querySql = "SELECT * "
            + "FROM "
            + " fact_order_retail_offline as retail_order "
            + " LEFT JOIN LATERAL TABLE(ORDER_DIM_DATE_TIME(cast(retail_order.submit_datetime as TIMESTAMP))) as submit_datetime ON true "
            + " LEFT JOIN `mysql`.`cube`.`dim_address` " + options
            + " FOR SYSTEM_TIME AS OF retail_order.proctime AS dim_address ON dim_address.address_id = retail_order.address_id "
            + " LEFT JOIN `mysql`.`cube`.`dim_ware` " + options
            + " FOR SYSTEM_TIME AS OF retail_order.proctime AS dim_ware ON dim_ware.ware_id = retail_order.ware_id ";
        Table dwdTable = tableEnv.sqlQuery(querySql);
        dwdTable.printSchema();
        tableEnv.createTemporaryView("dwd_order_retail_submit", dwdTable);

        String[] castFieldsArray = convertCastFields(tableEnv, "kudu", "impala_kudu", "dwd_order_retail_order_submit_test2");
        String castFields = String.join(",", castFieldsArray);
        System.out.println(castFields);

        String upsertToKuduSql = "UPSERT INTO `kudu`.`default_database`.`impala::impala_kudu.dwd_order_retail_order_submit_test2` SELECT "
                + castFields
                + " FROM "
                + " dwd_order_retail_submit";
        StatementSet statementSet = tableEnv.createStatementSet();
        statementSet.addInsertSql(upsertToKuduSql);
        statementSet.execute();
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
        tableEnv.createTemporarySystemFunction("PAY_DIM_DATE_TIME", new DimDateTimeFunction("pay"));        // 支付日期维度
    }

    private static void readMysqlTable(TableEnvironment tableEnv) throws TableNotExistException {
        CatalogBaseTable table = tableEnv.getCatalog("mysql").get().getTable(new ObjectPath("cube", "drugstore_quota_day"));
        TableSchema schema = table.getSchema();
        String[] fieldNames = schema.getFieldNames();
        System.out.println(Arrays.toString(fieldNames));
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
