package com.kad.cube_test.api_test;

public class TestString {
    public static void main(String[] args) {
        String str = "123456789";
        // 返回指定的子字符串在字符串中第一次出现的索引， fromIndex = 0表示从字符串的开始进行查找
        // 如果没有找到则返回 -1
        System.out.println(str.indexOf("-", 0));
    }
}
