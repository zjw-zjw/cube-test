package com.kad.cube_test.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.sql.Date;

// 零售订单表对象
@Data
@AllArgsConstructor
@NoArgsConstructor
public class OmOrder {
    private String ordercode;
    private String cuscode;
    private String cusname;
    private String cusgradeid;
    private Integer orderstatus;
    private Integer changetype;
    private String servicecode;
    private Integer salemode;
    private String sellercode;
    private Integer ordersource;
    private Integer orderchannel;
    private String originorderid;
    private String payconcode;
    private BigDecimal sumamt;
    private BigDecimal freightcost;
    private BigDecimal appendfreightcost;
    private BigDecimal disamt;
    private BigDecimal couponamt;
    private BigDecimal pointsamt;
    private BigDecimal netamt;
    private BigDecimal unpaidamt;
    private BigDecimal claimcountamt;
    private Integer stockstatus;
    private Integer issplitorder;
    private Integer issplitconsign;
    private String plansenddate;
    private String sendtime;
    private Integer sendpriotity;
    private Integer isspectransfer;
    private String transfercode;
    private String orderdesc;
    private String remark;
    private Integer issuspend;
    private String suspendtype;
    private String suspenddesc;
    private String suspendrelease;
    private Integer isrx;
    private Integer urgenttype;
    private Integer invoicetype;
    private String shopcode;
    private String orgcode;
    private String ip;
    private String nuid;
    private String sales;
    private String salescode;
    private String creator;
    private String creatorcode;
    private String createdate;
    private Integer ischeckmark;
    private Integer iscps;
    private String selecttype;
    private String gspshopcode;
    private String lastmodifytime;
    private BigDecimal prepayamt;
    private BigDecimal sellserveramt;
    private BigDecimal buyserveramt;
    private Long isthirdplate;
    private Long rxprocess;
    private String edittime;
    private BigDecimal freightinsurance;
    private BigDecimal exchangeinsurance;
}
