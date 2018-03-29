package com.rwj.offlineAnalysisPrj.spark.product;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.spark.sql.api.java.UDF1;

/**
 * Created by renwujie on 2018/03/26 at 16:16
 */
public class RemoveRandomPrefixUDF implements UDF1<String, String> {
    @Override
    public String call(String s) throws Exception {
        return s.split("_")[1];
    }
}
