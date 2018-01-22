package com.rwj.offlineAnalysisPrj

import com.rwj.offlineAnalysisPrj.constant.Constants
import com.rwj.offlineAnalysisPrj.spark.session.accumulator.SessionAggrStatAccumulator
import com.rwj.offlineAnalysisPrj.util.StringUtils
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by renwujie on 2018/01/15 at 15:16
  *
  * reference：
  *   1. http://itang.iteye.com/blog/1128707
  *
  * 思路：
  *   ① iszero()是否和初始值相同
  *   ② reset()重置值
  *   ③ copy() Creates a new copy of this accumulator, which is zero value. i.e. call `isZero` on the copy must return true.
  *   ④ add() Takes the inputs and accumulates. 指定累加逻辑
  *   ⑤ merge() Merges another same-type accumulator into this one and update its state. 指定的是多个task间聚合时的聚合逻辑
  */
object SessionAggrStatAccumulatorTest2 {
  def main(args: Array[String]): Unit = {
    /**
      * Scala中，自定义Accumulator
      * 使用object，直接定义一个伴生对象即可
      * 需要实现AccumulatorParam接口，并使用[]语法，定义输入输出的数据格式
      */
    object SessionAggrStatAccumulator extends AccumulatorV2[String, String] {

      private var result = Constants.SESSION_COUNT + "=0|" +
        Constants.TIME_PERIOD_1s_3s + "=0|" +
        Constants.TIME_PERIOD_4s_6s + "=0|" +
        Constants.TIME_PERIOD_7s_9s + "=0|" +
        Constants.TIME_PERIOD_10s_30s + "=0|" +
        Constants.TIME_PERIOD_30s_60s + "=0|" +
        Constants.TIME_PERIOD_1m_3m + "=0|" +
        Constants.TIME_PERIOD_3m_10m + "=0|" +
        Constants.TIME_PERIOD_10m_30m + "=0|" +
        Constants.TIME_PERIOD_30m + "=0|" +
        Constants.STEP_PERIOD_1_3 + "=0|" +
        Constants.STEP_PERIOD_4_6 + "=0|" +
        Constants.STEP_PERIOD_7_9 + "=0|" +
        Constants.STEP_PERIOD_10_30 + "=0|" +
        Constants.STEP_PERIOD_30_60 + "=0|" +
        Constants.STEP_PERIOD_60 + "=0"

      override def isZero: Boolean = {
        val newResult = Constants.SESSION_COUNT + "=0|" +
          Constants.TIME_PERIOD_1s_3s + "=0|" +
          Constants.TIME_PERIOD_4s_6s + "=0|" +
          Constants.TIME_PERIOD_7s_9s + "=0|" +
          Constants.TIME_PERIOD_10s_30s + "=0|" +
          Constants.TIME_PERIOD_30s_60s + "=0|" +
          Constants.TIME_PERIOD_1m_3m + "=0|" +
          Constants.TIME_PERIOD_3m_10m + "=0|" +
          Constants.TIME_PERIOD_10m_30m + "=0|" +
          Constants.TIME_PERIOD_30m + "=0|" +
          Constants.STEP_PERIOD_1_3 + "=0|" +
          Constants.STEP_PERIOD_4_6 + "=0|" +
          Constants.STEP_PERIOD_7_9 + "=0|" +
          Constants.STEP_PERIOD_10_30 + "=0|" +
          Constants.STEP_PERIOD_30_60 + "=0|" +
          Constants.STEP_PERIOD_60 + "=0"
        this.result == newResult
      }

      override def copy(): AccumulatorV2[String, String] = {
        val copyAccumulator = SessionAggrStatAccumulator
        copyAccumulator.result = this.result
        copyAccumulator
      }

      override def reset(): Unit = {
        result = Constants.SESSION_COUNT + "=0|" +
          Constants.TIME_PERIOD_1s_3s + "=0|" +
          Constants.TIME_PERIOD_4s_6s + "=0|" +
          Constants.TIME_PERIOD_7s_9s + "=0|" +
          Constants.TIME_PERIOD_10s_30s + "=0|" +
          Constants.TIME_PERIOD_30s_60s + "=0|" +
          Constants.TIME_PERIOD_1m_3m + "=0|" +
          Constants.TIME_PERIOD_3m_10m + "=0|" +
          Constants.TIME_PERIOD_10m_30m + "=0|" +
          Constants.TIME_PERIOD_30m + "=0|" +
          Constants.STEP_PERIOD_1_3 + "=0|" +
          Constants.STEP_PERIOD_4_6 + "=0|" +
          Constants.STEP_PERIOD_7_9 + "=0|" +
          Constants.STEP_PERIOD_10_30 + "=0|" +
          Constants.STEP_PERIOD_30_60 + "=0|" +
          Constants.STEP_PERIOD_60 + "=0"
      }

      override def add(v: String): Unit = {
        val v1 = this.result
        val v2 = v
        if (StringUtils.isEmpty(v1)) {
          v2
        } else {
          var newResult = ""
          val oldValue = StringUtils.getFieldFromConcatString(v1, "\\|", v2)
          if (StringUtils.isNotEmpty(oldValue)) {
            val newValue = Integer.valueOf(oldValue) + 1
            newResult = StringUtils.setFieldInConcatString(result, "\\|", v2, String.valueOf(newValue))
          }
          result = newResult
        }
      }

      override def merge(other: AccumulatorV2[String, String]): Unit = {
        if (other == null) {
          return
        } else {
          //TODO:  http://itang.iteye.com/blog/1128707
          if (other.isInstanceOf[SessionAggrStatAccumulator]) {
            val arr = Array(
              Constants.SESSION_COUNT,
              Constants.TIME_PERIOD_1s_3s,
              Constants.TIME_PERIOD_4s_6s,
              Constants.TIME_PERIOD_7s_9s,
              Constants.TIME_PERIOD_10s_30s,
              Constants.TIME_PERIOD_30s_60s,
              Constants.TIME_PERIOD_1m_3m,
              Constants.TIME_PERIOD_3m_10m,
              Constants.TIME_PERIOD_10m_30m,
              Constants.TIME_PERIOD_30m,
              Constants.STEP_PERIOD_1_3,
              Constants.STEP_PERIOD_4_6,
              Constants.STEP_PERIOD_7_9,
              Constants.STEP_PERIOD_10_30,
              Constants.STEP_PERIOD_30_60,
              Constants.STEP_PERIOD_60)
            arr.foreach { v =>
              val oldValue = StringUtils.getFieldFromConcatString(this.result, "\\|", v);
              if (StringUtils.isNotEmpty(oldValue)) {
                val newValue = oldValue.toInt + StringUtils.getFieldFromConcatString(other.value, "\\|", v).toInt;
                val newResult = StringUtils.setFieldInConcatString(result, "\\|", v, newValue.toString)
                newResult
              }
            }
          }
        }
      }

      override def value: String = {
        return this.result
      }
    }

    // 创建Spark上下文
    val conf = new SparkConf()
      .setAppName("SessionAggrStatAccumulatorTest")
      .setMaster("local")
    val sc = new SparkContext(conf);

    val accumulatorTst = new SessionAggrStatAccumulator
    sc.register(accumulatorTst)

    // 模拟使用一把自定义的Accumulator
    val arr = Array(Constants.TIME_PERIOD_1s_3s, Constants.TIME_PERIOD_4s_6s)
    val rdd = sc.parallelize(arr, 1)

    for(i <- 1 to 10 ) {
      rdd.foreach {
        accumulatorTst.add(_)
      }
    }

//    println(1 to 10)
    println(accumulatorTst.value)
  }
}