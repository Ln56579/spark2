package com.huoshan.spark.streaming.test

import java.util

import com.huoshan.spark.streaming.redis.{JedisUtil, PrintJedisAllData}

/**
  * Description : 显示数据库中  所有的数据
  * Created by ln on : 2018/8/22 9:17 
  * Author : ln56
  */
object JedisTest {
  def main(args: Array[String]): Unit = {

    PrintJedisAllData.printAll

  }
}
