package com.huoshan.spark.streaming.cms.Tage

import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row

/**
  * Description : 广告标签
  * Created by ln on : 2018/8/26 9:26 
  * Author : ln56
  */
object Tags4Ads extends Tags{
  /**
    * 打标签的逻辑
    */
  override def makeTags(args: Any*): Map[String, Int] = {
    var map =Map[String,Int]()
    val row = args.asInstanceOf[Row]
    val adTypeId = row.getAs[Int]("adspacetype")
    val adTypeName = row.getAs[String]("adspacetypename")
    if (adTypeId > 9) map += "LC"+adTypeId -> 1
    else if(adTypeId > 0) map += "LC0"+adTypeId -> 1

    if (StringUtils.isNotEmpty(adTypeName)) map +=  "LN"+adTypeName -> 1

    map
  }
}
