package com.huoshan.spark.streaming.cms.Tage

/**
  * Description : TODO
  * Created by ln on : 2018/8/26 9:24 
  * Author : ln56
  */
trait Tags {
  /**
    * 打标签的逻辑
    */
    def makeTags(args: Any*):Map[String,Int]
}
