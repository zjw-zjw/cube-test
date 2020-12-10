package com.kad.cube_test.api_test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

public class SortTest {
    public static void main(String[] args) {
        ArrayList<Long> longs = new ArrayList<>();
        longs.add(123L);
        longs.add(111L);
        longs.add(121L);
        longs.add(121L);
        longs.add(99L);
        System.out.println("排序前：" + longs.toString());
        Collections.sort(longs);
        System.out.println("排序后：" + longs.toString());
    }
}
