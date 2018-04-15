package com.rwj.offlineAnalysisPrj.test;

import com.rwj.offlineAnalysisPrj.util.SparkUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

/**
 * Created by renwujie on 2018/04/15 at 20:41
 */
public class SaveData {
    public static void main(String[] args){

        SparkSession ss = SparkUtils.getSparkSesseion("savedata");
        SQLContext sqlContext = ss.sqlContext();

        sqlContext.read().parquet("T:\\testdata\\sparkprj\\user_visit_action").registerTempTable("user_visit_action");

        Dataset product_info = ss.sql("select * from user_visit_action");
        product_info.toJavaRDD().coalesce(1).saveAsTextFile("T:\\testdata\\test");
    }
}
