package com.sql.spark_1.Demo

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * StructType 添加表头方式的
  */
object SQLDemo2 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("SQLDemo").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    val lines = sc.textFile("hdfs://ln1:9000/Odata/persion.txt")

    val rowRDD = lines.map(line => {
      val fields = line.split(",")
      val id = fields(0).toLong
      val name = fields(1)
      val age = fields(2).toInt
      val fv = fields(3).toDouble
      Row(id, name, age, fv)
    })
    //其实就是表头   用来描述DataFrame
    val schema: StructType = StructType(List(
      StructField("id",LongType,true),
      StructField("name",StringType,true),
      StructField("age",IntegerType,true),
      StructField("fv",DoubleType,true)
    ))

    val bdf: DataFrame = sqlContext.createDataFrame(rowRDD , schema )

    //先注册临时表
    bdf.registerTempTable("t_boy")
    //书写SQL     是一个Transformation
    val  result: DataFrame =sqlContext.sql("select * from t_boy order by fv desc ,age asc")

    result.show()

    sc.stop()
  }
}
