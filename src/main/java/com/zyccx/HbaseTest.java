package com.zyccx;


import java.io.IOException;

public class HbaseTest {
    public static void main(String[] args) throws IOException {
        System.out.println("hello world!");

//        HBaseDao.createTable("hello", new String[]{"info", "class"});
//        HBaseDao.addRecord("hello", "001", "info", "name", "zhang-san");
//        HBaseDao.addRecord("hello", "001", "info", "age", "21");
//        HBaseDao.addRecord("hello", "001", "info", "sex", "男");
//

//        HBaseDao.addRecord("hello","002","info","name","李四");
//        HBaseDao.addRecord("hello","002","info","age","18");
//        HBaseDao.addRecord("hello","002","info","sex","男");


//        HBaseDao.addRecord("hello", "002", "class", "年级", "三年二班");
//        HBaseDao.addRecord("hello", "002", "class", "位置", "1号楼2层");
//        HBaseDao.addRecord("hello", "002", "class", "班主任", "王老师");

        HBaseDao.get("hello", "002");

        HBaseDao.scan("hello");

    }

}
