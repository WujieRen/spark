package com.sparklearn.example_scala.cache.more_less

import org.apache.spark.sql.SparkSession

/**
  * Created by renwujie on 2018/01/19 at 16:08
  *
  * 虽然只在map里进行了累加器加1的操作，但是两次得到的累加器的值却不一样。
  *
  * 这是由于count和reduce都是action类型的操作，触发了两次作业的提交，所以map算子实际上被执行了了两次，在reduce操作提交作业后累加器又完成了一轮计数，所以最终累加器的值为18。
  *
  * 究其原因是因为count虽然促使numberRDD被计出来，但是由于没有对其进行缓存，所以下次再次需要使用numberRDD这个数据集时，还需要从并行化数据集的部分开始执行计算。解释到这里，这个问题的解决方法也就很清楚了，就是在count之前调用numberRDD的cache方法（或persist），这样在count后数据集就会被缓存下来，reduce操作就会读取缓存的数据集而无需从头开始计算了。
  */
object More {
  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder()
      .master("local")
      .appName("testCache")
      .getOrCreate()
    val sc = ss.sparkContext

    val accum = sc.longAccumulator("less")

    val sumRDD = sc.parallelize(Array(1, 2, 3, 4, 5, 6, 7, 8, 9), 2).map(n => {
      accum.add(1L)
      n + 1
    })

//    sumRDD.count()
    sumRDD.cache().count()
    println("accum1 : " + accum.value)
    sumRDD.reduce(_+_)
    println("accum2 : " + accum.value)

  }
}
