package com.huoshan.partition.simple

import com.huoshan.partition.OrderingType.{SortRules, XianRou}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object SortTuple {
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
    val sorted = userRDD.sortBy(tp => (-(tp._3),tp._2))

    val r = sorted.collect()

    println(r.toBuffer)

    sc.stop()
  }


}
