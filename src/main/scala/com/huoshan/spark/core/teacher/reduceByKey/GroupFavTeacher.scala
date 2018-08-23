package com.huoshan.reduceByKey

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GroupFavTeacher {
  def main(args: Array[String]): Unit = {
    //参数个数判断
    if (args.length!=2) {
      println(
        """
          |com.huoshan.reduceByKey.GroupFavTeacher
          |
          |Parameter Expect:
          |
          |      inputPath        D:\SparkTest\InputPath\teacher.log
          |      take -> TopN              3
          |
        """.stripMargin
      )
      sys.exit()
    }
    val Array(inputPath,take) = args
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

    val lines: RDD[String] = sc.textFile(inputPath)
    val subjectTeacher = lines.map(line => {
      //截取
      val splits: Array[String] = line.split("/")
      val teacher = splits(3)
      val subject = splits(2).split("[.]")(0)

      ((subject,teacher), 1)
    })
    //这种方法不好 调用了俩次map方法
    //val map: RDD[((String, Int), Int)] = subjectAndTeacher.map((_,1))
    val reduced = subjectTeacher.reduceByKey(_+_)

    //分组排序(按学科分组)
    val grouped = reduced.groupBy(_._1._1)
   // val grouped: RDD[(String, Iterable[((String, String), Int)])] = reduced.groupBy((t:))
    //经过分组之后  一个分区内(一个学科就是一个迭代器)  可能有多个学科的数据
    //将每一个组拿出来进行排序
    //[key学科   :  value  学科的数据]                                                             内存加磁盘
    val sorted= grouped.mapValues(_.toList.sortBy(_._2).reverse.take(take.toInt))  //_.tolist数据特别大     RangPatitioner   先抽样
          //一个学科的数据  都在一个scala集合里面了                take  是从exqtor 计算好在拉回前几个  再排序            全局排序
    val r: Array[(String, List[((String, String), Int)])] = sorted.collect()                     // filter 过滤数据

    println(r.toBuffer)

    sc.stop()
  }
}
