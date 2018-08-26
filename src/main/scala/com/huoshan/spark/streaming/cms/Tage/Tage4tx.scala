package com.huoshan.spark.streaming.cms.Tage

import com.huoshan.spark.streaming.cms.util.TageUtil
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Description : TODO
  * Created by ln on : 2018/8/26 8:52 
  * Author : ln56
  */
object Tage4tx {
  def main(args: Array[String]): Unit = {
    //参数个数判断
    if (args.length!=4) {
      println(
        """
          |com.huoshan.spark.streaming.cms.Tage.Tage4tx
          |
          |Parameter Expect:
          |
          |      inputPath
          |      appDis
          |      stopWordDis
          |      outputPath
        """.stripMargin
      )
      sys.exit()
    }
    val Array(inputPath,appDis,stopWordDis,outputPath) = args
    //构建sc
    val conf = new SparkConf()
      .setAppName(s"${this.getClass.getSimpleName}")
      .setMaster("local[4]")
    //RDD的序列化   worker 和 worker直接的通信
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //初始化 sc
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    //设置 日志级别
    sc.setLogLevel(logLevel = "WARN")
    //分离sc初始化   和    正式执行代码
    println("\n\n"+("--"*15)+"  sc init success  "+("--"*15)+"\n\n")
    //字典文件 ---> app字典
    val disMap = sc.textFile(appDis).map(line => {
      val fields = line.split("\t", -1)
      (fields(4), fields(1))
    }).collect().toMap
    //字典文件--->停用字典
    val stopWord = sc.textFile(stopWordDis).map((_,0)).collect().toMap

    //广播变量
    val appDisMap = sc.broadcast(disMap)
    val stopWordMap = sc.broadcast(stopWord)

    sqlContext.read.parquet(inputPath).where(TageUtil.hasSomeUserIdConditition)
    sc.stop()
  }
}
