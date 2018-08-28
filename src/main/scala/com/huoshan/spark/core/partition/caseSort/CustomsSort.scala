package com.huoshan.partition.caseSort

import com.huoshan.partition.sort.Boy
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object CustomsSort {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("CustomSort").setMaster("local[4]")
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
    //TODO 传入的是排序规则,不会改变样式  只会改变顺序      case class 不用new
    val sorted = userRDD.sortBy( tp =>Man(tp._2,tp._3))

    val r = sorted.collect()

    println(r.toBuffer)

    sc.stop()
  }
}
