package com.sparklearn.example_scala.cache

import org.apache.spark.sql.SparkSession

/**
  * Created by renwujie on 2018/01/18 at 17:16
  *
  * reference:
  *   http://bit1129.iteye.com/blog/2182146
  *     1.Spark不支持ShuffleMapRDD的cache()/collect()
  */
object SparkWordCountCache {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\hadoop-2.5.0");
    val ss = SparkSession.builder()
      .master("local[2]")
      .appName("SparkWordCountCache")
      .getOrCreate()
    val sc = ss.sparkContext

    val rdd1 = sc.textFile("data/cache/word.txt")
    val rdd2 = rdd1.flatMap(_.split(" "))
    val rdd3 = rdd2.map((_, 1))
    rdd3.saveAsTextFile("data/cache/output/rdd3shuffle/" + System.currentTimeMillis())
//    val result1 = rdd3.collect()
    val result1 = rdd3.cache()
    result1.foreach(println)
//    val rdd4 = rdd3.reduceByKey(_+_, 3)
    val rdd4 = rdd3.reduceByKey(_+_)
    //Spark是不支持ShuffleMapRDD的cache的，虽然上面不需要ShuffleMapTask，但是ResultTask运行时，依然需要从MapTask的结果中拉取数据。
    rdd4.cache()
    rdd4.saveAsTextFile("data/cache/output/rdd4/" + System.currentTimeMillis())
    val result = rdd4.collect()
//    result.foreach(println(_))
    sc.stop()

  }
}
