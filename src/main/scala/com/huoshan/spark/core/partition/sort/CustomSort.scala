package com.huoshan.partition.sort

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CustomSort {
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
      new user(name, age, fv)
    })
    val sorted = userRDD.sortBy( u => u)

    val r = sorted.collect()

    println(r.toBuffer)
    
    sc.stop()
  }
}
class user(val name : String ,val age : Int ,val fv :Int ) extends Ordered[user] with Serializable {
  override def compare(that: user): Int = {
    if (this.fv == that.fv){
      this.age - that.age
    }else {
      -(this.fv-that.fv)
    }
  }

  override def toString: String = s"name:$name,age:$age,fv:$fv"
}