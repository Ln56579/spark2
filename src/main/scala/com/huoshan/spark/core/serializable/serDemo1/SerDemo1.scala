package com.huoshan.serializable.serDemo1

import java.net.InetAddress

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SerDemo1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SerDemo1")
    val sc = new SparkContext(conf)
    val lines: RDD[String] = sc.textFile(args(0))

    val r = lines.map(word => {
      val rules = new Rules()
      //TODO    测试   在哪个机子上执行   在哪个线程执行
      val hostName = InetAddress.getLocalHost.getHostName
      val threadName = Thread.currentThread().getName
      (hostName,threadName,rules.rulesMap.getOrElse(word,0),rules.toString)
    })

    r.saveAsTextFile(args(1))

    sc.stop()
    
  }
}
