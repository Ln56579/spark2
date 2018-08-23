package com.huoshan.spark.core.demo.wordCount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Description : scala   WordCount
  *                       union   并集
  *                       intersection  交集                        初始值  ||abc|def 计算三次
  *                       aggregate  小分区聚合   在全局聚合    aggregate("")(Math.max(_,_),(_+_))
  *                       aggregateByKey  在小分区  内用一次   俩个分区都出现  就会是200 一个分区出现就是100
  *                       filterByRange   根据条件过滤
  *
  *                       distinct ( 去重 )聚合   属于suffer
  *
  *                       foreachPartition 一下子拿出来一个分区
  *                       mapValues 一下拿出一个迭代器
  * Created by ln on 2018/7/20
  * Author : ln56
  */
object ScalaWordCount {

  def main(args: Array[String]): Unit = {
    //参数个数判断
    if (args.length!=2) {
      println(
        """
          |com.huoshan.spark.core.demo.wordCount.ScalaWordCount
          |
          |Parameter Expect:
          |
          |      inputPath
          |      outputPath
          |
        """.stripMargin
      )
      sys.exit()
    }
    val Array( inputPath ,outputPath ) = args
    //创建spark配置
    val conf = new SparkConf().setAppName(s"${this.getClass.getSimpleName}")
                              .setMaster("local[4]")
    //Spark的入口
    val sc = new SparkContext(conf)
    //指定从哪里读取内容  并创建RDD(弹性分别式集合)
    val lines: RDD[String] = sc.textFile(inputPath)
    //切分压平
    val words: RDD[String] = lines.flatMap(_.split(" "))
    //和1进行组合
    val wordAndOne: RDD[(String, Int)] = words.map((_,1))
    //安key集合
    val reduced: RDD[(String, Int)] = wordAndOne.reduceByKey(_+_)
    //排序
    val sorted: RDD[(String, Int)] = reduced.sortBy(_._2,ascending = false)
    //将结果保存到HDFS
    sorted.saveAsTextFile(outputPath)
    //释放资源
    sc.stop()

  }
}
