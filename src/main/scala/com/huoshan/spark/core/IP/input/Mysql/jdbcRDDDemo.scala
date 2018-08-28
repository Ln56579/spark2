package com.huoshan.input.Mysql

import java.sql.DriverManager

import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}

object jdbcRDDDemo {
  def main(args: Array[String]): Unit = {

   val getConn = () =>{
        DriverManager.getConnection("jdbc:mysql://ln1:3306/ln?charatorEncoding=utf-8","root","Xm123456@")
   }

    val conf = new SparkConf().setAppName("jdbcRDDDemo").setMaster("local[4]")

    val sc = new SparkContext(conf)

    //创建RDD,这个RDD会记录以后从mysql中的数据     RDD记录从哪里都数据  没有真正的数据

    var jdbcRDD = new JdbcRDD(
      sc,
      getConn,             //TODO
      "select * from ACCESS_LOG where count>= ? and count <= ? ",
      1,
      3000,
      2,
      rs => {
        val city = rs.getString(1)
        val count = rs.getInt(2)
        (city,count)
      }
    )
    val jdbcDemo = jdbcRDD.collect()

    println(jdbcDemo.toBuffer)

    sc.stop()
  }
}
