package com.huoshan.Thread.ThrDemo1

import java.text.SimpleDateFormat

class FilterUtilV1 {
   val dataFormat = new SimpleDateFormat("yyyy年MM月dd日,E,HH:mm:ss")

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
