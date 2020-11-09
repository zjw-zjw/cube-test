package com.kad.cube_test.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;


import java.sql.Date;
import java.sql.Timestamp;

// 零售订单表对象
@Data
@AllArgsConstructor
@NoArgsConstructor
public class OmOrder {
    public String ordercode;
    public String cuscode;
    public String cusname;
    public String cusgradeid;
    public Integer orderstatus;
    public Integer changetype;
    public String servicecode;
    public Integer salemode;
    public String sellercode;
    public Integer ordersource;
    public Integer orderchannel;
    public String originorderid;
    public String payconcode;
    public Double sumamt;
    public Double freightcost;
    public Double appendfreightcost;
    public Double disamt;
    public Double couponamt;
    public Double pointsamt;
    public Double netamt;
    public Double unpaidamt;
    public Double claimcountamt;
    public Integer stockstatus;
    public Integer issplitorder;
    public Integer issplitconsign;
    public Timestamp plansenddate;
    public Timestamp sendtime;
    public Integer sendpriotity;
    public Integer isspectransfer;
    public String transfercode;
    public String orderdesc;
    public String remark;
    public Integer issuspend;
    public String suspendtype;
    public String suspenddesc;
    public Timestamp suspendrelease;
    public Integer isrx;
    public Integer urgenttype;
    public Integer invoicetype;
    public String shopcode;
    public String orgcode;
    public String ip;
    public String nuid;
    public String sales;
    public String salescode;
    public String creator;
    public String creatorcode;
    public Timestamp createdate;
    public Integer ischeckmark;
    public Integer iscps;
    public String selecttype;
    public String gspshopcode;
    public Timestamp lastmodifytime;
    public Double prepayamt;
    public Double sellserveramt;
    public Double buyserveramt;
    public Long isthirdplate;
    public Long rxprocess;
    public Timestamp edittime;
    public Double freightinsurance;
    public Double exchangeinsurance;
}
