package com.rwj.offlineAnalysisPrj.util;

import com.rwj.offlineAnalysisPrj.conf.ConfiguratoinManager;
import com.rwj.offlineAnalysisPrj.constant.Constants;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

/**
 * Created by renwujie on 2018/01/05 at 16:28
 *
 * Spark工具类
 */
public class SparkUtils {

    /**
     * 根据当前是否本地测试的配置，
     *
     *  决定如何设置SparkConf的master
     *  决定是否支持HiveContext
     */
    public static SparkSession getSparkSesseion(String appName) {
        boolean local = ConfiguratoinManager.getBooleanValue(Constants.SPARK_LOCAL);

        System.out.println(local);

        SparkSession ss = null;
        SparkSession.Builder builder = SparkSession.builder();
        builder
                //.config("spark.default.parallelism", "100")
                .config("spark.storage.memoryFraction", "0.5")
                .config("spark.shuffle.consolidateFiles", "true")
                .config("spark.shuffle.file.buffer", "64")
                .config("spark.shuffle.memoryFraction", "0.3")
                .config("spark.reducer.maxSizeInFlight", "24")
                .config("spark.shuffle.io.maxRetries", "60")
                .config("spark.shuffle.io.retryWait", "60");

        if(local) {

            ss = builder.master("local")
                    .appName(appName)
                    .getOrCreate();
        } else {
            ss = builder.appName(appName)
                    .enableHiveSupport()
                    .getOrCreate();
        }

        return ss;
    }

    /**
     * 生成模拟数据
     * 如果spark.local配置设置为true，则生成模拟数据；否则不生成
     * @param sc
     * @param sqlContext
     */
    public static void mockData(JavaSparkContext sc, SQLContext sqlContext) {

    }


}
