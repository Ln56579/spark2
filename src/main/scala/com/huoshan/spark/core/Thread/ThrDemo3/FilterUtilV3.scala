package com.huoshan.Thread.ThrDemo3

import java.text.SimpleDateFormat

import org.apache.commons.lang3.time.FastDateFormat


object FilterUtilV3 {

  //dataFormat   是线程不安全的
   //val dataFormat = new SimpleDateFormat("yyyy年MM月dd日,E,HH:mm:ss")
    //线程安全的  原理还是枷锁
    val dataFormat = FastDateFormat.getInstance("yyyy年MM月dd日,E,HH:mm:ss")

    //共享的成员变量      就会出现 线程不安全


   def filterByType(fields :Array[String],tp:String) ={
      val _tp = fields(0)
     _tp == tp
   }
    def filterByTime(fields :Array[String],startTime:Long ,endTime:Long) ={
      val time = fields(1)
      val timeLong = dataFormat.parse(time).getTime
      timeLong >= startTime && timeLong <= endTime
    }
}
