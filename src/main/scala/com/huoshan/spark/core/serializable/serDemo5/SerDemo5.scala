package com.huoshan.serializable.serDemo5

import java.net.InetAddress

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SerDemo5 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("SerDemo4")
    val sc = new SparkContext(conf)
    val lines: RDD[String] = sc.textFile(args(0))

    val r = lines.map(word => {

      //TODO    测试   在哪个机子上执行   在哪个线程执行
      val hostName = InetAddress.getLocalHost.getHostName
      val threadName = Thread.currentThread().getName
      (hostName,threadName,Rules5.rulesMap.getOrElse(word,0),Rules5.toString)
    })

    r.saveAsTextFile(args(1))

    sc.stop()
    
  }
}
