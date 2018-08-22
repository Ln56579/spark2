package com.huoshan.spark.streaming.redis

/**
  * Description : 显示数据库中  所有的数据
  * Created by ln on : 2018/8/22 9:58 
  * Author : ln56
  */
object PrintJedisAllData {

  def printAll= {

    val jedis = JedisUtil.getJedis()

    val keySet = jedis.keys("*")
    if (keySet!=null){
      val keys = keySet.iterator()

      while (keys.hasNext){
        //next  是拿数据的   只能调用一次
        val key = keys.next()
        val value: String = jedis.get(key)

        println(key+" ---> " +value)
      }
    }else{
      print("database not  data")
    }

    jedis.close()
  }

}
