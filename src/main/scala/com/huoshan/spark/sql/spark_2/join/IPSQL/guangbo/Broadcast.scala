package com.sql.spark_2.join.IPSQL.guangbo

import com.sql.spark_2.join.IPSQL.Demo.MyUtil1
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Description : join的代价很昂贵 , 而且非常慢 , 解决思路先把表缓存起来 ( 广播变量 )
  * Created by ln on : 2018/8/1 14:44 
  * Author : ln56
  */
object Broadcast {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("IPLocaltionSQL").master("local[*]").getOrCreate()
    import spark.implicits._
    val rules: Dataset[String] = spark.read.textFile(args(0))
    //在driver端获取所有的IP规则然后广播出去
    val rulesDataset = rules.map(line => {
      val fileds = line.split("[|]")
      val startNum = fileds(2).toLong
      val endNum = fileds(3).toLong
      val province = fileds(6)
      (startNum, endNum, province)
    })
    //将规则收集到Driver
    val rulesInDriver: Array[(Long, Long, String)] = rulesDataset.collect()
    //广播
    val broadcastRef: Broadcast[Array[(Long, Long, String)]] = spark.sparkContext.broadcast(rulesInDriver)

    val lines: Dataset[String] = spark.read.textFile(args(1))

    val ips = lines.map(line => {
      val fieds = line.split("[|]")
      val ip = fieds(1)
      //将ip转换成10进制的数
      val ipNum = MyUtil2.ip2Long(ip)
      ipNum
    }).toDF("ip_num")

    ips.createTempView("v_log")

    //TODO         自定义一个函数 , 并注册
     spark.udf.register("ip2Province",(ipNum : Long) => {
       //查询ip的规则(事先已经广播出去,已经在Executor中了)
       val ipRulesInExecutor: Array[(Long, Long, String)] = broadcastRef.value
        //根据ip对应的十进制数查找省份名称
        val index = MyUtil2.binarySearch(ipRulesInExecutor,ipNum)
        var province = "火星"
       if (index != -1){
          province = ipRulesInExecutor(index)._3
       }
       province
     })

    //执行sql
    val r = spark.sql("select ip2Province(ip_num) province,count(*) counts from v_log group by province order by counts desc")

    r.show()

    spark.stop()
  }
}
