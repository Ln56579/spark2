package com.sql.spark_2.join.Demo

import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * Description :
  * Created by ln on : 2018/8/1 09:52 
  * Author : SYSTEM
  */
object DataFrameSQL {
  def main (args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("sparkSQL").master("local[*]").getOrCreate()

    import spark.implicits._
    val lines: Dataset[String] = spark.createDataset(List("1,laozhao,cn","2,laoli,usa","3,laoyang,jp"))
    val tpDs = lines.map(word => {
      val fuleds = word.split(",")
      val id = fuleds(0).toLong
      val name = fuleds(1)
      val ename = fuleds(2)
      (id, name, ename)
    })
    val df1 = tpDs.toDF("id","name","ename")
    val useTb: Dataset[String] = spark.createDataset(List("cn,中国","usa,美国","jp,日本"))

    val tpDs1 = useTb.map( flued => {
      val word = flued.split(",")
      val cation = word(0)
      val cname = word(1)
      (cation,cname)
    })
    val df2 = tpDs1.toDF("cation","cname")

    val r = df1.join(df2,$"ename" === $"cation")

    r.show()

    spark.stop()
    

  }
}
