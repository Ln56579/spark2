package com.huoshan.cache.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  *   cache的底层方法  可以保存 内存   磁盘   和  内存+磁盘
  */
object CacheAndDisk {
  def main(args: Array[String]): Unit = {
    //TODO 数组
    val subjects = Array("bigdata","javaee","php")

    val conf = new SparkConf().setAppName("faTeacher").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile(args(0))
    val subjectTeacher = lines.map(line => {

      val splits: Array[String] = line.split("/")
      val teacher = splits(3)
      val subject = splits(2).split("[.]")(0)

      ((subject,teacher), 1)
    })

    val reduced: RDD[((String, String), Int)] = subjectTeacher.reduceByKey(_+_)
    //RDD的sortBy     内存 + 磁盘       过滤数据       该RDD中对应的数据  仅有一个学科的数据
   // reduced.sortBy(_._2,false)     按照老师和学科  统一排序
    //TODO for循环   遍历数组    分多次提交   防止内存溢出
    //标记为RDD被反复使用

    //reduced.filter()     先过滤在cache

    val cached = reduced.persist(StorageLevel.MEMORY_ONLY)

    for (sb <- subjects){

      val filter: RDD[((String, String), Int)] = cached.filter(_._1._1 == sb )

      val sorted: RDD[((String, String), Int)] = filter.sortBy(_._2,false)
      // TODO      take  : action方法      在 Executor 中计算好了  在传回 driver 端
      val favTeacher: Array[((String, String), Int)] = sorted.take(3)

      println(favTeacher.toBuffer)
    }

    cached.unpersist(true)        //(同步  异步释放false)
    sc.stop()
  }
}
