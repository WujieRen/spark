package com.rwj.offlineAnalysisPrj.spark.product;

import org.apache.spark.sql.api.java.UDF3;

/**
 * 用指定分隔符拼接两个字段
 * Created by renwujie on 2018/03/20 at 20:11
 */
public class ConcatLongStringUDF implements UDF3<Long, String, String, String> {
    @Override
    public String call(Long v1, String v2, String split) throws Exception {
        return v1 + split + v2;
    }
}
