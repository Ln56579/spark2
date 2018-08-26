package com.huoshan.spark.core.cache.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  *   RangePartitioner is OverallSort
  *   缓存到内存中  速度最快
  */
object Cache_100 {
  def main(args: Array[String]): Unit = {
    //TODO 数组
    val subjects = Array("bigdata","javaee","php")
    //参数个数判断
    if (args.length!=1) {
      println(
        """
          |com.huoshan.reduceByKey.GroupFavTeacher
          |
          |Parameter Expect:
          |
          |      inputPath        D:\SparkTest\InputPath\teacher.log
          |
        """.stripMargin
      )
      sys.exit()
    }
    val Array(inputPath) = args
    //构建sc
    val conf = new SparkConf()
      .setAppName(s"${this.getClass.getSimpleName}")
      .setMaster("local[4]")
    //RDD的序列化   worker 和 worker直接的通信
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //初始化 sc
    val sc = new SparkContext(conf)
    //设置 日志级别
    sc.setLogLevel(logLevel = "WARN")
    //分离sc初始化   和    正式执行代码
    println("\n\n"+("--"*15)+"  sc init success  "+("--"*15)+"\n\n")

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

    //reduced.filter()     先过滤在cache         unpersist 释放数据

    val cached = reduced.cache()

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
