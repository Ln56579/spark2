package com.sql.spark_1.DataFrame

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Description : DataFrame上执行sparkSQL
  * Created by ln on : 2018/7/31 20:28 
  * Author : ln56
  */
object DataFrameDemo {
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

    val df1: DataFrame = bdf.select("name","age","fv")
    //sort   ==   orderBy   2.0 统一了
    import sqlContext.implicits._
    val df2: Dataset[Row] = df1.orderBy($"fv" desc,$"age" asc)


    df2.show()

    sc.stop()
  }
}
