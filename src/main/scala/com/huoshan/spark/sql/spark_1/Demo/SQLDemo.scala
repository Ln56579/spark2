package com.sql.spark_1.Demo

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object SQLDemo {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("SQLDemo").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    val lines = sc.textFile("hdfs://ln1:9000/Odata/persion.txt")

    val mingRDD = lines.map(line => {
      val fields = line.split(",")
      val id = fields(0).toLong
      val name = fields(1)
      val age = fields(2).toInt
      val fv = fields(3).toDouble
      Ming(id, name, age, fv)
    })
    //该RDD封装了ming的数据,有了shcma的信息,但是还是一个RDD
    //导入隐士转换          
    import sqlContext.implicits._
    val bdf: DataFrame = mingRDD.toDF

    //先注册临时表
    bdf.registerTempTable("t_ming")
    //书写SQL     是一个Transformation
    val  result: DataFrame =sqlContext.sql("select * from t_ming order by fv desc ,age asc")

    result.show()

    sc.stop()
  }
}
case class Ming(id:Long,name:String,age:Int,fv:Double)