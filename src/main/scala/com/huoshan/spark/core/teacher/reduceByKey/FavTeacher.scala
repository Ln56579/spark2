package com.huoshan.reduceByKey

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object FavTeacher {
  def main(args: Array[String]): Unit = {
    //参数个数判断
    if (args.length!=1) {
      println(
        """
          |com.huoshan.reduceByKey.FavTeacher
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


    val lines: RDD[String] = sc.textFile(inputPath)
    val teacherAsOne = lines.map(line => {
      //截取
      val splits: Array[String] = line.split("/")

      val teacher = splits(3)
      (teacher, 1)
    })
    //聚合
    val reducer: RDD[(String, Int)] = teacherAsOne.reduceByKey(_+_)
    //排序
    val sorted: RDD[(String, Int)] = reducer.sortBy(_._2,false)
    //收集
    val result: Array[(String, Int)] = sorted.collect()

    //打印
    val arr = result.toBuffer

    println(arr)

    sc.stop()

  }
}
