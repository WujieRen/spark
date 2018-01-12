package com.rwj.offlineAnalysisPrj.util;

import com.rwj.offlineAnalysisPrj.conf.ConfiguratoinManager;
import com.rwj.offlineAnalysisPrj.constant.Constants;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

/**
 * Created by renwujie on 2018/01/05 at 16:28
 *
 * Spark工具类
 */
public class SparkUtils {

    /**
     * 根据当前是否本地测试的配置
     * 决定，如何设置SparkConf的master
     */


    /**
     * 获取SQLContext
     * 如果spark.local设置为true，那么就创建SQLContext；否则，创建HiveContext
     * @param sc
     * @return
     */


    /**
     * 生成模拟数据
     * 如果spark.local配置设置为true，则生成模拟数据；否则不生成
     * @param sc
     * @param sqlContext
     */
    public static void mockData(JavaSparkContext sc, SQLContext sqlContext) {
        boolean local = ConfiguratoinManager.getBooleanValue(Constants.SPARK_LOCAL);
        if(local){
            //MockData.mock(sc, sqlContext);
        }
    }


}
