package com.scalabook

import org.apache.spark.sql.SparkSession

/**
  * Created by renwujie on 2018/01/15 at 20:17
  *
  * 1、获取数据，看下啥样
  */
object Test {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[*]").appName("Test").getOrCreate()

    val srcData = spark.read.csv("hdfs://testenv:8020/testdata/spark/reilly/linkage")

    println(srcData.first().toString())

    spark.close()
  }
}