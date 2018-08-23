package com.huoshan.spark.streaming.redis

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
  * Description : Jedis 连接池
  * Created by ln on : 2018/8/21 21:53
  * Author : ln56
  */
object JedisUtil {
  private val config = new JedisPoolConfig
  private val jedisPool = new JedisPool(config,"47.106.182.143",6379,5000,null,7)

  def getJedis () : Jedis = {
    jedisPool.getResource
  }
}
