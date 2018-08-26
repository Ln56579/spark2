package com.huoshan.spark.streaming.cms.util

/**
  * Description : TODO
  * Created by ln on : 2018/8/26 9:20 
  * Author : ln56
  */
object TageUtil {

  val hasSomeUserIdConditition={
    """
      |imel != "" or imelmd5 != "" imelsha1 != "" or
      |idfa != "" or idfamd5 != "" idfasha1 != "" or
      |mac != "" or macmd5 != "" macsha1 != "" or
      |androidid != "" or androididmd5 != "" androididsha1 != "" or
      |openudid != "" or openudidmd5 != "" openudidsha1 != ""
    """.stripMargin
  }
}
