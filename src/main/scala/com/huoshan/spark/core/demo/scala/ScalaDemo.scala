package com.huoshan.spark.core.demo.scala

/**
  * Description : scala  Map
  * Created by ln on 2018/7/10
  * Author : ln56
  */
object ScalaDemo {

  def main(args: Array[String]): Unit = {
    //声明变量    1.   val   i   =     100      (不可变的,变量的引用不可变)
    //          2.   var     str1  :String = "JAVA" (可变的)
    val  str = "qqq"
    println(str)
    for(i        <-        1 to 10){
      println(i)
    }
    for (i   <-  1  to  3 ;  j   <-   1  to  3    if(i!=j)){
      println(i*10+j)
    }
    val  res  = for(i     <-     1     until    10   )  yield    i   //创建一个集合   然后把所有的数据放进去
    println(res)

    val arr = new Array[Int](8)
    print(arr)
  }

}
