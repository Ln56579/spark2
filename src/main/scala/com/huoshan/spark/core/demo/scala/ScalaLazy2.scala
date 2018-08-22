package com.huoshan.spark.core.demo.scala

/**
  * Description : lazy      关键字的测试
  * Created by ln on : 2018/7/11 10:28
  * Author : ln56
  */

object ScalaLazy2 {
  def init():Unit = {
    println("执行了init方法")
  }
  def main(args: Array[String]): Unit = {
    lazy val property = init()  //没有用lazy修饰的         ()   是Unit   的返回类型
    println("after init()")
    println(property)
  }
}
