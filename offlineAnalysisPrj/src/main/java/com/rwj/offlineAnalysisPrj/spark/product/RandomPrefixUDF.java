package com.rwj.offlineAnalysisPrj.spark.product;

import org.apache.spark.sql.api.java.UDF2;

import java.util.Random;

/**
 * Created by renwujie on 2018/03/26 at 16:06
 */
public class RandomPrefixUDF implements UDF2<String, Integer, String> {

    @Override
    public String call(String val, Integer num) throws Exception {
        Random random = new Random(0);
        int randomNum = random.nextInt(10);
        return randomNum + "_" + val;
    }
}
