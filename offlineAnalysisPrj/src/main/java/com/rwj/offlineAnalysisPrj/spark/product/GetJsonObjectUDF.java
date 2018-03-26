package com.rwj.offlineAnalysisPrj.spark.product;

import com.alibaba.fastjson.JSONObject;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;

/**
 * Created by renwujie on 2018/03/21 at 12:10
 */
public class GetJsonObjectUDF implements UDF2<String, String, String> {
    @Override
    public String call(String json, String field) throws Exception {
        try {
            JSONObject jsonObject = JSONObject.parseObject(json);
            return jsonObject.getString(field);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }
}
