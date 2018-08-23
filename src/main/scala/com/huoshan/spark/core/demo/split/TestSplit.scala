package com.huoshan.spark.core.demo.split

import java.net.URL

object TestSplit {

  def main(args: Array[String]): Unit = {

    val line = "http://bigdata.edu360.cn/laozhao"

    val splits: Array[String] = line.split("/")

    val subjectLong = splits(2)
    val teacher = splits(3)
    val subject = subjectLong.split("[.]")(0)
    //  学科           老师
    println(subject+"    "+teacher)


    val index = line.lastIndexOf("/")
    val teacher2 = line.substring(index+1)   //   /laozhao  所以 index + 1

    val subject2 = line.substring(0,index) //http://bigdata.edu360.cn
    val hostname = new URL(subject2).getHost

    println(teacher2+"    "+subject2+"     "+hostname)

  }
}
