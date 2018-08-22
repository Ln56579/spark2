package com.huoshan.spark.streaming.redis

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Description : 将文件 存入到 Redis 中
  * Created by ln on : 2018/8/21 22:01
  * Author : ln56
  */
object testFile2Redis {
  def main(args: Array[String]): Unit = {
    //参数个数判断
    if (args.length != 1) {
      println(
        """
          |com.huoshan.spark.streaming.testFile2Redis
          |
          |Parameter Expect:
          |
          |      inputPath    D:\SparkTest\InputPath\graph.txt
          |
        """.stripMargin
      )
      sys.exit()
    }
    val Array(inputPath) = args
    //创建spark配置
    val conf = new SparkConf().setAppName(s"${this.getClass.getSimpleName}")
      .setMaster("local[4]")
    //RDD的序列化   worker 和 worker直接的通信
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //Spark的入口
    val sc = new SparkContext(conf)

    val data = sc.textFile(inputPath).map( line => {
      val arr = line.split("\t",-1)
      val userName = arr.filter(!_.contains(":"))
      val tagesName = arr.filter(_.contains(":"))
      userName.map(name => {
        ( name , tagesName(0) )
      })
    }).foreachPartition(itr => {
      val jedis = JedisUtil.getJedis()
      itr.foreach(t => {
        for (s <- t){
          jedis.set(s._1,s._2)
        }
      })
      jedis.close()
    })
    sc.stop()
  }
}
