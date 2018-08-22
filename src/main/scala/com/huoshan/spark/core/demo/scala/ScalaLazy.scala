package com.huoshan.spark.core.demo.scala

/**
  * Description : Scala 中用lazy定义的变量叫惰性变量.会实现延迟加载       只能是不可变的变量
  *                     且只有在调用惰性变量时, 才会实例化这个变量
  * Created by ln on : 2018/7/11 10:23
  * Author : ln56
  */
object ScalaLazy {

  def init():Unit = {
    println("执行了init方法")
  }
  def main(args: Array[String]): Unit = {
    val property = init()  //没有用lazy修饰的         ()   是Unit   的返回类型
    println("after init()")
    println(property)
  }
}

