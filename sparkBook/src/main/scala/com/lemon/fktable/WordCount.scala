package com.lemon.fktable

import org.apache.spark.sql.SparkSession

/**
  * Created by renwujie on 2018/01/29 at 9:54
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder()
      .master("local")
      .appName("WordCount")
      .getOrCreate()
    val sc = ss.sparkContext

    val words = sc.textFile("data/com.lemon.fktable/table.txt")
    val result = words.flatMap(line => line.split(" "))
      .filter(word => word.nonEmpty)
      .map((_,1))
      .reduceByKey(_+_)

//    result.foreach(print)
    result.foreach{v =>
      print(v + " ")
    }
  }
}
