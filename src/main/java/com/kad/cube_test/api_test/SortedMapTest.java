package com.kad.cube_test.api_test;

import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;

public class SortedMapTest {
    public static void main(String[] args) {
        TreeMap<Long, String> treeMap = new TreeMap<>();
        Random r = new Random(1);
        for (int i = 0; i < 10; i++) {
            treeMap.put((long) r.nextInt(10), "haha-" + i);
        }
//        treeMap.put(3L, "三");
//        treeMap.put(6L, "六");
//        treeMap.put(1L, "一");
//        treeMap.put(2L, "二");
//        treeMap.put(0L, "零");

        treeMap.forEach((k, v) -> System.out.println("k:" + k.toString() + " v: " + v.toString()));
    }
}
