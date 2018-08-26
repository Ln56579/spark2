package com.huoshan.spark.core.cache.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Description : TODO
  * Created by ln on : 2018/8/26 10:35 
  * Author : ln56
  */
object lineCount {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("lineCount").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile(args(0))
    //TODO    被存内存中 cache
    val line = lines.cache()

    val lineNum: Long = line.count()

    println(lineNum)


    sc.stop()

    }
}
