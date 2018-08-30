package com.sql.spark_2.join.IPSQL.math

import org.apache.spark.sql
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
  * Description : 几何平均数 (自定义聚合函数)
  * Created by ln on : 2018/8/1 15:35 
  * Author : SYSTEM
  */
object UdafTest {
  def main (args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("UdafTest").master("local[*]").getOrCreate()

    val range = spark.range(1,11)

    val gm = new GaoMean        //不知道gm是什么
    spark.udf.register("gm",gm)
    //将range 注册成一个视图
    range.createTempView("v_range")

    val r = spark.sql("select gm(id) result from v_range")

    r.show()

    spark.stop()
  }
}

