package com.rwj.offlineAnalysisPrj.spark.session.sortByKeyScala

import org.apache.spark.sql.SparkSession

/**
  * Created by renwujie on 2018/01/24 at 17:53
  */
object SortKeyTest {
  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder()
      .master("local")
      .appName("SortKeyTest")
      .getOrCreate()
    val sc = ss.sparkContext

    val arr = Array(Tuple2(new SortKey(23, 34, 56), "3"),
      Tuple2(new SortKey(38, 34, 56), "1"),
      Tuple2(new SortKey(23, 35, 56), "2"))
    val rdd = sc.parallelize(arr, 1)

    val sortedRDD = rdd.sortByKey()
//    val sortedRDD = rdd.sortByKey(false)

    for(tuple <- sortedRDD.collect()) {
      println(tuple._2)
    }
  }
}
