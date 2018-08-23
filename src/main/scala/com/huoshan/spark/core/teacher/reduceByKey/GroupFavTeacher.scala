package com.huoshan.reduceByKey

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GroupFavTeacher {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("faTeacher").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile(args(0))
    val subjectTeacher = lines.map(line => {
      //截取
      val splits: Array[String] = line.split("/")
      val teacher = splits(3)
      val subject = splits(2).split("[.]")(0)

      ((subject,teacher), 1)
    })
    //这种方法不好 调用了俩次map方法
    //val map: RDD[((String, Int), Int)] = subjectAndTeacher.map((_,1))
    val reduced: RDD[((String, String), Int)] = subjectTeacher.reduceByKey(_+_)

    //分组排序(按学科分组)
    val grouped: RDD[(String, Iterable[((String, String), Int)])] = reduced.groupBy(_._1._1)
   // val grouped: RDD[(String, Iterable[((String, String), Int)])] = reduced.groupBy((t:))
    //经过分组之后  一个分区内(一个学科就是一个迭代器)  可能有多个学科的数据
    //将每一个组拿出来进行排序
    //[key学科   :  value  学科的数据]                                                             内存加磁盘
    val sorted= grouped.mapValues(_.toList.sortBy(_._2).reverse.take(3))  //_.tolist数据特别大     RangPatitioner   先抽样
          //一个学科的数据  都在一个scala集合里面了                take  是从exqtor 计算好在拉回前几个  再排序            全局排序
    val r: Array[(String, List[((String, String), Int)])] = sorted.collect()                     // filter 过滤数据

    println(r.toBuffer)

    sc.stop()
  }
}
