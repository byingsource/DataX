package com.snow99.bigdata.etl;

import com.alibaba.datax.core.Engine;


import java.text.SimpleDateFormat;
import java.util.Date;

public class DataXTest {

    public static void start(String dataxPath,String jsonPath) throws Throwable {
        System.setProperty("datax.home", dataxPath);
        System.setProperty("now", new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss:SSS").format(new Date()));// 替换job中的占位符
        //String[] datxArgs = {"-job", dataxPath + "/job/text.json", "-mode", "standalone", "-jobid", "-1"};
        String[] datxArgs = {"-job", jsonPath, "-mode", "standalone", "-jobid", "-1"};
//        Engine.entry(datxArgs);
        Engine.main(datxArgs);
    }

    public static void main(String[] args) throws Throwable {

        start("D:\\Java\\datax", "D:\\code\\java\\trunk\\dmex\\wallet-analysis\\dataxconf_dev\\chia_coin_record_sqlite.json");
    }
}
