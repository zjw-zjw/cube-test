package com.kad.cube_test.hive;

import com.alibaba.fastjson.JSONObject;
import com.kad.cube_test.model.OmOrder;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.types.Row;

import java.sql.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class OmOrderSourceFunction<T> extends RichSourceFunction<Row> {
    Connection con = null;// 创建一个数据库连接
    PreparedStatement pre = null;// 创建预编译语句对象，一般都是用这个而不用Statement
    ResultSet result = null;// 创建一个结果集对象
    String[] oracleDataTypeList = new String[] {"VARCHAR2", "NUMBER", "NVARCHAR2", "DATE"};

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");   // 加载Oracle驱动程序
            System.out.println("开始尝试连接数据库！");

            String url = "jdbc:oracle:" + "thin:@192.168.1.23:1521:kaderp";
            String user = "dc";
            String password = "dc360kad";
            con = DriverManager.getConnection(url, user, password);
            System.out.println("连接成功！");
            String sql = "select * from om.OM_ORDER WHERE rownum <= 100";
            pre = con.prepareStatement(sql);
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public void run(SourceContext<Row> ctx) throws Exception {
        result = pre.executeQuery();
        ResultSetMetaData metaData = result.getMetaData();
        int columnCount = metaData.getColumnCount();
        Map<String, String> columnType = new HashMap<>();

        for (int i = 0; i < columnCount; i++) {
            String columnTypeName = metaData.getColumnTypeName(i+1);
//            Arrays.asList(oracleDataTypeList)
            String columnName = metaData.getColumnName(i+1).toLowerCase();
            columnType.put(columnName, columnTypeName);
        }
        System.out.println("type:" + columnType.toString());

        while (result.next()) {
            Map<String, Object> map = new HashMap<>();
            for (int i = 0; i < columnCount; i++) {
                String col_name = metaData.getColumnName(i+1).toLowerCase();
                Object col_value = result.getObject(col_name);
                map.put(col_name, col_value);
            }

//            System.out.println(map.toString());
            OmOrder omOrder = JSONObject.parseObject(JSONObject.toJSONString(map), OmOrder.class);
            Row of = Row.of(omOrder);
            ctx.collect(of);
        }
    }

    @Override
    public void cancel() {

    }

    @Override
    public void close() throws Exception {
        super.close();
        try{
            // 逐一将上面的几个对象关闭，因为不关闭的话会影响性能、并且占用资源
            // 注意关闭的顺序，最后使用的最先关闭
            if (result != null)
                result.close();
            if (pre != null)
                pre.close();
            if (con != null)
                con.close();
            System.out.println("数据库连接已关闭！");
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}


//            // 当结果集不为空时
//            OmOrder omOrder = new OmOrder();

//            result.getString("ordercode");
//            result.getString("CUSCODE");
//            result.getString("CUSNAME");
//            result.getString("CUSGRADEID");
//            result.getString("ORDERSTATUS");
//            result.getString("CHANGETYPE");
//            result.getString("SERVICECODE");
//            result.getString("SALEMODE");
//            result.getString("SELLERCODE");
//            result.getString("ORDERSOURCE");
//            result.getString("ORDERCHANNEL");
//            result.getString("ORIGINORDERID");
//            result.getString("PAYCONCODE");
//            result.getString("SUMAMT");
//            result.getString("FREIGHTCOST");
//            result.getString("APPENDFREIGHTCOST");
//            result.getString("DISAMT");
//            result.getString("COUPONAMT");
//            result.getString("POINTSAMT");
//            result.getString("NETAMT");
//            result.getString("UNPAIDAMT");
//            result.getString("CLAIMCOUNTAMT");
//            result.getString("STOCKSTATUS");
//            result.getString("ISSPLITORDER");
//            result.getString("ISSPLITCONSIGN");
//            result.getString("PLANSENDDATE");
//            result.getString("SENDTIME");
//            result.getString("SENDPRIOTITY");
//            result.getString("ISSPECTRANSFER");
//            result.getString("TRANSFERCODE");
//            result.getString("ORDERDESC");
//            result.getString("REMARK");
//            result.getString("ISSUSPEND");
//            result.getString("SUSPENDTYPE");
//            result.getString("SUSPENDDESC");
//            result.getString("SUSPENDRELEASE");
//            result.getString("ISRX");
//            result.getString("URGENTTYPE");
//            result.getString("INVOICETYPE");
//            result.getString("SHOPCODE");
//            result.getString("ORGCODE");
//            result.getString("IP");
//            result.getString("IP");
//            result.getString("NUID");
//            result.getString("SALES");
//            result.getString("SALESCODE");
//            result.getString("CREATOR");
//            result.getString("CREATORCODE");
//            result.getString("CREATEDATE");
//            result.getString("ISCHECKMARK");
//            result.getString("ISCPS");
//            result.getString("SELECTTYPE");
//            result.getString("GSPSHOPCODE");
//            result.getString("LASTMODIFYTIME");
//            result.getString("PREPAYAMT");
//            result.getString("SELLSERVERAMT");
//            result.getString("BUYSERVERAMT");
//            result.getString("ISTHIRDPLATE");
//            result.getString("RXPROCESS");
//            result.getString("EDITTIME");
//            result.getString("FREIGHTINSURANCE");
//            result.getString("EXCHANGEINSURANCE");