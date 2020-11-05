package com.kad.cube_test.api_test;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;

/**
 *  研究 java.u
 */
public class TimeStampTest {
    public static void main(String[] args) throws ParseException {
        Timestamp timestamp = string2Time("2020-10-16 09:49:26.000");
        System.out.println(timestamp);
    }

    /**
     *method 将字符串类型的日期转换为一个timestamp（时间戳记java.sql.Timestamp）
     dateString 需要转换为timestamp的字符串
     dataTime timestamp
     */
    public final static java.sql.Timestamp string2Time(String dateString) throws ParseException {
        DateFormat dateFormat;
        dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS", Locale.ENGLISH);//设定格式
        //dateFormat = new SimpleDateFormat("yyyy-MM-dd kk:mm:ss", Locale.ENGLISH);
        dateFormat.setLenient(false);
        java.util.Date timeDate = dateFormat.parse(dateString);     //util类型
        java.sql.Timestamp dateTime = new java.sql.Timestamp(timeDate.getTime());   //Timestamp类型,timeDate.getTime()返回一个long型
        return dateTime;
    }
}
