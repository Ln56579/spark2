package com.huoshan.spark.core.demo.scala

/**
  * Description : scala List
  * Created by ln on : 2018/7/9 9:23
  * Author : ln56
  */
object ScalaList {

  def main(args: Array[String]): Unit = {
    val list0 = List(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
    val list1 = list0.map(_ * 2) //把你的值循环出来   做操作
    val list2 = list0.filter(_ % 2 == 0) //过滤数据
    val list3 = list0.sorted    //排序
    val list4 = list3.reverse   //反转
    val it = list0.grouped(4)   //转成一个It
    val list5 = it.toList     //转成list
    val list6 = list5.flatten  //压平

    val lines = List("hello java h","java i a","scala aw qw")
    val words = lines.flatMap(_.split(" "))        //切分并且压平
    //并行    --->     某一时间点   同时多线程一起计算
    val arr1 = Array(1,2,3,4,5,6,7,8,9,10)
    val sum1 = arr1.par.sum      //和线程有关   每个线程运算一部分(1+2+3+4)+(5+6+7+8)+(9+10)
    val res = arr1.reduce(_+_)     // ((1+2)+3)+...
    val res1 = arr1.par.reduce(_-_)


    println(list1)
    println(list2)
    println(list3)
    println(list4)
    println("----------------------------------------------")
    println(it.toBuffer)
    println(list5)
    println(list6)
    println(words)
    println(arr1.sum)
    println("----------------------------------------------")
    println(sum1)
    println(res1)
  }

}
