package com.sparklearn.example_scala.cache.more_less.exam

import java.util

import org.apache.spark.util.AccumulatorV2

/**
  * Created by renwujie on 2018/01/19 at 16:31
  */
class LogAccumulator extends AccumulatorV2[String, util.Set[String]]{

  private val _logArray:java.util.Set[String] = new util.HashSet[String]()

  override def isZero: Boolean = {
    _logArray.isEmpty
  }

  override def copy(): AccumulatorV2[String, util.Set[String]] = {
    val newAcc = new LogAccumulator
    //TODO:这里不理解
    _logArray.synchronized{
      newAcc._logArray.addAll(_logArray)
    }
    newAcc
  }

  override def reset(): Unit = {
    _logArray.clear()
  }

  override def add(v: String): Unit = {
    _logArray.add(v)
  }

  override def merge(other: AccumulatorV2[String, util.Set[String]]): Unit = {
    other match {
      case o: LogAccumulator => _logArray.addAll(o.value)
    }
  }

  override def value: util.Set[String] = {
    util.Collections.unmodifiableSet(_logArray)
  }
}
