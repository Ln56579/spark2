package com.huoshan.spark.core.teacher.Executors.Partitioner

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * Description : 自定义分区器
  * Created by ln on : 2018/8/23 16:50 
  * Author : ln56
  */
object CustomPartition {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("CustomPartition").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile(args(0))
    val subjectTeacher = lines.map(line => {

      val splits: Array[String] = line.split("/")
      val teacher = splits(3)
      val subject = splits(2).split("[.]")(0)

      ((subject,teacher), 1)
    })
    //TODO    suffer    -->   (  reduceByKey  ,  partitionBy  )   一共俩次suffer
    val reduced: RDD[((String, String), Int)] = subjectTeacher.reduceByKey(_+_)
    //计算有多少学科         distinct ( 去重 )聚合   属于suffer
    val subjects: Array[String] = reduced.map(_._1._1).distinct().collect()

    val sbPartition = new SubjectPartitioner(subjects)

    //TODO   自定义分区器( suffer 的时候会用的到) 决定上游的到下游那个分区里面

    val partitioned = reduced.partitionBy(sbPartition)     //按照指定的分区器  分区
    //  一个学科  对应一个   分区       mapPartitions一次拿一个分区        .iterator把Array 转成iterator
    //TODO   mapPartitions 操作一个分区的数据
    val sorted = partitioned.mapPartitions(it => {
      it.toList.sortBy(_._2).reverse.take(3).iterator
    })
    println(sorted.collect().toBuffer)


    sc.stop()
  }
}
