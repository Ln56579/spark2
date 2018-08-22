package com.huoshan.spark.core.demo.scala

import java.net.URL

/**
  * Description : scala ---> spilt  逻辑
  * Created by ln on : 2018/7/12 10:25
  * Author : ln56
  */
object ScalaSpilt {

  def main(args: Array[String]): Unit = {
    val line = "http://bigdata.edu360.cn/laozhao"

    val splits: Array[String] = line.split("/")

    val subjectLong = splits(2)
    val teacher = splits(3)
    val subject = subjectLong.split("[.]")(0)
    //  学科           老师
    println(subject+"    "+teacher)


    val index = line.lastIndexOf("/")
    val teacher2 = line.substring(index+1)

    val subject2 = line.substring(0,index)
    val hostname = new URL(subject2).getHost
    println(teacher2+"    "+subject2+"     "+hostname)
  }

}
