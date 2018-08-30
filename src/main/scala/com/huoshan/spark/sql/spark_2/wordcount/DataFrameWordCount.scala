package com.sql.spark_2.wordcount

import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * Description : SQL - WordCount
  * Created by ln on : 2018/7/31 20:52 
  * Author : SYSTEM
  */
object DataFrameWordCount {
  def main (args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SQLWordCount").master("local[*]").getOrCreate()

    //指定从哪里读数据   也是lazy
    val lines: Dataset[String] = spark.read.textFile("hdfs://ln1:9000/ln")
    //DataSet  也是一个分布式数据集    对RDD进一步封装      只有一列默认是Value
    import spark.implicits._   //(_.split(" "))     需要导入隐士转换
    val words: Dataset[String] = lines.flatMap(_.split(" "))

    //val frame = words.groupBy($"value" as "word").count().sort($"count" desc)   (1)
    //val frame = words.groupBy($"value" as "word").count().count()//第二个count是action
    //agg  导入聚合函数                                                            (2)
    import org.apache.spark.sql.functions._
    val frame =  words.groupBy($"value" as "word").agg(count("*") as "counts" ).sort($"counts" desc)

    frame.show()

    spark.stop()
  }
}
