package com.sparklearn.example_scala.cache.more_less

import org.apache.spark.sql.SparkSession

/**
  * Created by renwujie on 2018/01/19 at 16:08
  *
  * 执行完毕，打印的值是多少呢？答案是0，因为累加器不会改变spark的lazy的计算模型，即在打印的时候像map这样的transformation还没有真正的执行，从而累加器的值也就不会更新。
  */
object Less {
  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder()
      .master("local")
      .appName("testCache")
      .getOrCreate()
    val sc = ss.sparkContext

    val accum = sc.longAccumulator("less")

    val sum = sc.parallelize(Array(1, 2, 3, 4, 5, 6, 7, 8, 9), 2).map(n => {
      accum.add(1L)
      n + 1
    })

    println("sum :" + sum)
    println("accum : " + accum.value)

  }
}
