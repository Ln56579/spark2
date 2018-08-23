package com.huoshan.spark.core.teacher.Executors.LessSuffer

import com.huoshan.spark.core.teacher.Executors.Partitioner.SubjectPartitioner
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

/**
  * Description :     reduceByKey(在这里传入分区器)  ,  partitionBy    减少suffer
  *                   inputPath:  HDFS       local
  *                   topN     :  take(topN)
  * Created by ln on : 2018/8/23 20:31 
  * Author : ln56
  */
object SufferLess {

  def main(args: Array[String]): Unit = {
    val topN = args(1).toInt

    val conf = new SparkConf().setAppName("CustomPartition").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile(args(0))
    val subjectTeacher = lines.map(line => {

      val splits: Array[String] = line.split("/")
      val teacher = splits(3)
      val subject = splits(2).split("[.]")(0)

      ((subject,teacher), 1)
    })
    //计算有多少学科         distinct ( 去重 )聚合   属于suffer
    val subjects: Array[String] = subjectTeacher.map(_._1._1).distinct().collect()


    val sbPartition = new SubjectPartitioner(subjects)
    //TODO    suffer    -->   (  reduceByKey(在这里传入分区器)  ,  partitionBy(去除)  )   一共俩次suffer
    val reduced: RDD[((String, String), Int)] = subjectTeacher.reduceByKey( sbPartition , _+_)

    //TODO   求top几就创建一个几个长度的 TreeSet
    val sorted = reduced.mapPartitions(it => {
      //it.toList.sortBy(_._2).reverse.take(3).iterator

      var arr = new ArrayBuffer[((String, String), Int)]()
      while( it.hasNext == true){
        // TODO   没有解决的地方    解决问题     TreeSet 使用方法
        if (arr.size <= topN + 1){
          arr += it.next()
          arr = arr.sortBy(_._2).reverse
          if (arr.size >= topN + 1) {
            arr.remove(topN)
          }
        }
      }
      arr.iterator
    })

    println(sorted.collect().toBuffer)
    //sorted.saveAsTextFile("D:/out")
    sc.stop()
  }
}
