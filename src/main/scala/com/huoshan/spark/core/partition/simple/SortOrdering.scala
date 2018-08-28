package com.huoshan.partition.simple

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SortOrdering {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("SortTuple").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val user = Array("laoduan 30 99","laozhao 29 9999","laozhang 28 99","laowang 30 87")
    val lines: RDD[String] = sc.parallelize(user)

    val userRDD = lines.map(line => {
      val fields = line.split(" ")
      val name = fields(0)
      val age = fields(1).toInt

      val fv = fields(2).toInt
      (name, age, fv)
    })
    //元组的比较     TODO 最少的代码

    //on[(String,Int,Int)]    原始 数据格式
    //Ordering[Int,Int]       转换后 数据格式
    //(t => ( -t._3, t._2)    比较规则
    implicit val rules = Ordering[(Int,Int)].on[(String,Int,Int)](t => (-t._3,t._2))
    val sorted = userRDD.sortBy(tp => tp)

    val r = sorted.collect()

    println(r.toBuffer)

    sc.stop()
  }


}
