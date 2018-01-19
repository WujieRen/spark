package com.sparklearn.example_scala.cache.more_less.exam

import org.apache.spark.sql.SparkSession

/**
  * Created by renwujie on 2018/01/19 at 16:31
  */
object Main {
  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder()
      .master("local")
      .appName("Main")
      .getOrCreate()
    val sc = ss.sparkContext

    val accum = new LogAccumulator
    sc.register(accum, "testLogAccumulator")

    val sum = sc.parallelize(Array("1", "2a", "3", "4b", "5", "6", "7cd", "8", "9"), 2).filter(line => {
      val pattern = """^-?(\d+)"""
      val flag = line.matches(pattern)
      if(!flag) {
        accum.add(line)
      }
      flag
    }).map(_.toInt).reduce(_ + _)

    println("sum : " + sum)
    //TODO:accum怎么遍历？
    println(accum.value)

    sc.stop()

  }
}
