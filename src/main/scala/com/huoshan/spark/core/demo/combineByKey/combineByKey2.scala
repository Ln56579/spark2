package com.huoshan.spark.core.demo.combineByKey

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object combineByKey2 {
  def main(args: Array[String]): Unit = {
    //构建sc
    val conf = new SparkConf()
      .setAppName("combineByKey")
      .setMaster("local[4]")
    //RDD的序列化   worker 和 worker直接的通信
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //初始化 sc
    val sc = new SparkContext(conf)
    //设置 日志级别
    sc.setLogLevel(logLevel = "WARN")
    //分离sc初始化   和    正式执行代码
    println("\n\n"+("--"*15)+"  sc init success  "+("--"*15)+"\n\n")

    val rdd1 = sc.parallelize(List(1,1,1,2,2,1,2,2,1),3)
    val rdd2 = sc.parallelize(List("dog","cat","gun","salmon","rabbit","turkey","wolf","beach","ass"),3)
    val rdd3 = rdd1.zip(rdd2)
    //TODO   combineByKey时修改分区数
    val rdd4 = rdd3.combineByKey(x => ListBuffer(x),   //局部的集合     分组分为 1 2 3 4 5 组
                      (m :ListBuffer[String] ,n :String )=> m += n ,  //局部聚合
                      ( a :ListBuffer[String],b :ListBuffer[String] )=>a ++= b ,new HashPartitioner(2),true,null)      //全局把局部的聚合聚合
                                                                                                                  //在map端是否合并         序列号器
   val result = rdd4.collect()

    val arr = result.toBuffer
    println(arr)

    sc.stop()
  }
}
