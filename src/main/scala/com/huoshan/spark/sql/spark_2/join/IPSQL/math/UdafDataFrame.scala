package com.sql.spark_2.join.IPSQL.math

import org.apache.spark.sql.SparkSession

/**
  * Description : 自定义聚合 函数     UDF   UDAF   UDTF(flatmap就搞定了  Spark中没有)
  * Created by ln on : 2018/8/1 16:16 
  * Author : SYSTEM
  */
object UdafDataFrame {
  def main (args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("UdafTest").master("local[*]").getOrCreate()

    val geomean = new GaoMean

    val range = spark.range(1,11)
    import spark.implicits._
    val r = range.agg(geomean($"id")).as("geomean")

    r.show()

    spark.stop()
  }
}
