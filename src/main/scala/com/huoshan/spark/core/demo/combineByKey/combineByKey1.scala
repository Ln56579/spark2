package com.huoshan.spark.core.demo.combineByKey

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object combineByKey1 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("combineByKey").setMaster("local[4]")

    val sc = new SparkContext(conf)

    sc.setLogLevel(logLevel = "WARN")

    val pairRDD: RDD[(String, Int)] = sc.parallelize(List(("hello",2),("jerry",1),("tom",3),("hello",4)))
    //生成一个 ShuffledRDD
    val copairRDD: RDD[(String, Int)] = pairRDD.combineByKey(x => x, (m:Int, n:Int) => m+n, (a:Int, b:Int) => a+b)

    val result = copairRDD.collect

    val arr = result.toBuffer
    println(arr)

    sc.stop()

  }
}
