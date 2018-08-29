package com.huoshan.serializable.serDemo3

import java.net.InetAddress

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SerDemo3 {
  def main(args: Array[String]): Unit = {

    val rules = new Rules3()     //driver端生成的对象    在excutor中执行必须序列号


    val conf = new SparkConf().setAppName("SerDemo1")
    val sc = new SparkContext(conf)
    val lines: RDD[String] = sc.textFile(args(0))

    val r = lines.map(word => {

      //TODO    测试   在哪个机子上执行   在哪个线程执行
      val hostName = InetAddress.getLocalHost.getHostName
      val threadName = Thread.currentThread().getName
      (hostName,threadName,rules.rulesMap.getOrElse(word,0),rules.toString)
    })

    r.saveAsTextFile(args(1))

    sc.stop()
    
  }
}
