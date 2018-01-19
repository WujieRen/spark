package com.sparklearn.example_scala.cache.more_less

import org.apache.spark.sql.SparkSession

/**
  * Created by renwujie on 2018/01/19 at 16:07
  */
object Zhengchang {
  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder()
      .master("local")
      .appName("testCache")
      .getOrCreate()
    val sc = ss.sparkContext

    val accum = sc.longAccumulator("zhengchang")

    val sum = sc.parallelize(Array(1,2,3,4,5,6,7,8,9), 2).filter(n => {
        if(n % 2 == 0) accum.add(1L)
        n % 2 == 0
    }).reduce(_+_)

    println("sum : " + sum)
    println("accum : " + accum.value)

  }
}
