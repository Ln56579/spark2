package com.huoshan.cache.checkpoint

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  *   RangePartitioner is OverallSort
  *   把中间结果保存到HDFS中    只要用于迭代计算
  */
object CacheHDFS {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("faTeacher").setMaster("local[4]")
    val sc = new SparkContext(conf)

    sc.setCheckpointDir("hdfs://ln1:9000/ln/Checkpoint")

    val lines: RDD[String] = sc.textFile(args(0))
    val filtered = lines.filter(_.contains("javaee"))

    filtered.checkpoint()     //checkpoint也是标志  需要触发 action
    filtered.count()

    sc.stop()
  }
}
