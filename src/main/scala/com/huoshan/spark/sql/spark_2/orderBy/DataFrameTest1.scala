package com.sql.spark_2.orderBy

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Description : Spark_2.x SQL的API   (sparkSession)
  * Created by ln on : 2018/7/31 20:38 
  * Author : ln56
  */
object DataFrameTest1 {
  def main (args: Array[String]): Unit = {
    val session: SparkSession = SparkSession
                        .builder()
                        .appName("SQLTest1")
                        .master("local[*]")
                        .getOrCreate()
    //创建RDD
    val lines: RDD[String] = session.sparkContext.textFile("hdfs://ln1:9000/Odata/persion.txt")

    val rowRDD = lines.map(line => {
      val fields = line.split(",")
      val id = fields(0).toLong
      val name = fields(1)
      val age = fields(2).toInt
      val fv = fields(3).toDouble
      Row(id, name, age, fv)
    })
    val schema: StructType = StructType(List(
      StructField("id",LongType,true),
      StructField("name",StringType,true),
      StructField("age",IntegerType,true),
      StructField("fv",DoubleType,true)
    ))
    //创建DataFrame

    val df = session.createDataFrame(rowRDD,schema)

    import session.implicits._             //隐式转换
    val df1 = df.where($"fv" > 98)

    val selected = df1.select("name","age","fv")

    val sorted = selected.orderBy($"fv" desc ,$"age" asc)

    sorted.show()        //数据太多  show也不好     需要保存起来

//    sorted.write.jdbc()

    session.stop()

  }
}
