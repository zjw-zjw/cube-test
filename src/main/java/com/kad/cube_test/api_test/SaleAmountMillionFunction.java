package com.kad.cube_test.api_test;


import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.util.Preconditions;

import java.lang.reflect.Array;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.stream.Collectors;

public class SaleAmountMillionFunction<T> extends AggregateFunction<T[], LinkedList<T>> {
    private static final long serialVersionUID = 5196358706919877311L;
    private static final double MILLION = 5000;
    private final int length;
    private final TypeInformation<T> typeInfo;
    double sumAmount = 0;      //累加值
    long lastProcessTime = 0; //逻辑上的最后一次处理时间
    long regulartime = 0;    // 判断是否到第二天的时间

    List<Long> submitDateList = new ArrayList<>(60 * 24); //存放submit_date
    Map<Long,Double> map = new HashMap<>();//存放submit_date对应的amount

    public SaleAmountMillionFunction(TypeInformation<T> typeInfo, int length) {
        Preconditions.checkNotNull(typeInfo, "Type information");
        this.length = length;
        this.typeInfo = typeInfo;
    }

    @Override
    public T[] getValue(LinkedList<T> acc) {
        List<T> result = new ArrayList<>(length);
        for (T value : acc) {
            result.add(value);
        }
        return result.toArray(createArray(result.size()));
    }

    @Override
    public LinkedList<T> createAccumulator() {
        LinkedList acc = new LinkedList();
        return acc;
    }

    public void accumulate(LinkedList<T> acc, LocalDateTime submitdate, String ts, double value) throws ParseException {
        //String类型日期转成long类型时间戳
        Calendar cal = Calendar.getInstance();
        cal.setTime(new SimpleDateFormat("yyyy-MM-dd HH:mm").parse(ts));
        long current = cal.getTimeInMillis();

        //long类型时间戳转成String类型日期
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");

        long today = submitdate.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        System.out.println("today:" + today);

        if (regulartime < today) {
            sumAmount = 0;
            submitDateList.clear();
            map.clear();
            acc.clear();
        }
        regulartime = today;
        System.out.println("regularTime = day: " + regulartime);

        if (current < lastProcessTime) {  // 乱序数据
            if (map.containsKey(current)) {
                Double aDouble = map.get(current);
                aDouble = value;
//                valueList.clear();
//                valueList.add(value);
                map.put(current, aDouble);
                lateDataProcess(acc, sdf);
            } else {
                submitDateList.add(current);
                Collections.sort(submitDateList);
                map.put(current, value);
                lateDataProcess(acc, sdf);
            }
        } else if (current > lastProcessTime){
            sumAmount += value;
            System.out.println("当前累计值：" + sumAmount);
            submitDateList.add(current);

            map.put(current, value);
            lastProcessTime = current;
            while (sumAmount >= MILLION) {
                String date = sdf.format(new Date(current));
                acc.add((T) date.substring(11, 16));
                sumAmount = sumAmount - MILLION;
            }
        } else {
            // 当前时间 等于 上一次时间
            Double lastValue = map.get(current);
            sumAmount = sumAmount - lastValue + value;
            System.out.println("current == lastProcess 当前累计值：" + sumAmount);
            map.put(current, value);
            while (sumAmount >= MILLION) {
                String date = sdf.format(new Date(current));
                System.out.println("相等时 Date 值 ：" + date);
                acc.add((T) date.substring(11, 16));
                sumAmount = sumAmount - MILLION;
            }
        }
    }

    public void retract(LinkedList<T> acc, LocalDateTime submitdate, String ts, double value) {
//        System.out.println(value);
    }


    private T[] createArray(int length) {
        return (T[]) Array.newInstance(typeInfo.getTypeClass(), length);
    }

    @Override
    @SuppressWarnings("unchecked")
    public TypeInformation<LinkedList<T>> getAccumulatorType() {
        // try {
        Class<LinkedList<T>> clazz = (Class<LinkedList<T>>) (Class) LinkedList.class;
        return TypeInformation.of(clazz);
    }

    @Override
    public TypeInformation<T[]> getResultType() {
        return Types.OBJECT_ARRAY(this.typeInfo);
    }

    public void lateDataProcess(LinkedList<T> acc, SimpleDateFormat sdf) {
        sumAmount = 0;
        acc.clear();
        for (int i = 0; i < submitDateList.size(); i++) {
            Long time = submitDateList.get(i);
            Double doubles = map.get(time);
            sumAmount += doubles;
            while (sumAmount >= MILLION) {
                String date = sdf.format(new Date(time));
                sumAmount = sumAmount - MILLION;
                acc.add((T) date.substring(11, 16));
            }
        }
    }

}
