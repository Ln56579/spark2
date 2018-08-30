package com.sql.spark_2.wordcount

import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * Description : SQL - WordCount
  * Created by ln on : 2018/7/31 20:52 
  * Author : SYSTEM
  */
object SQLWordCount {
  def main (args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SQLWordCount").getOrCreate()

    //指定从哪里读数据   也是lazy
    val lines: Dataset[String] = spark.read.textFile(args(0))
    //DataSet  也是一个分布式数据集    对RDD进一步封装      只有一列默认是Value
    import spark.implicits._   //(_.split(" "))     需要导入隐士转换
    val words: Dataset[String] = lines.flatMap(_.split(" "))

    words.createTempView("v_wc")

    //执行sql (Transformation , lazy)
    val result = spark.sql("SELECT value,COUNT(*) counts FROM v_wc GROUP BY value ORDER BY counts DESC")

    result.show()

    spark.stop()
  }
}
