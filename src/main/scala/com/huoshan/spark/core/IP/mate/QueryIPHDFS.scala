package com.huoshan.IP.mate

import com.huoshan.IP.BinarySearch.MyUtil
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object QueryIPHDFS {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("QueryIPHDFS").setMaster("local[4]")

    val sc = new SparkContext(conf)
    //在driver端获取所有的IP规则然后广播出去
    val RulesLines = sc.textFile(args(0))
    val ipRulesRDD = RulesLines.map(line => {
      val fileds = line.split("[|]")
      val startNum = fileds(2).toLong
      val endNum = fileds(3).toLong
      val province = fileds(6)
      (startNum, endNum, province)
    })
    val rules = ipRulesRDD.collect()

    val broadcastRef: Broadcast[Array[(Long, Long, String)]] = sc.broadcast(rules)

    val lines: RDD[String] = sc.textFile(args(1))

    val func = (lines : String) => {
      val fieds = lines.split("[|]")
      val ip = fieds(1)
      //将ip转换成10进制的数
      val ipNum = MyUtil.ip2Long(ip)
      //进行二分法查找,通过广播变量
      val rulesInExecutor: Array[(Long, Long, String)] = broadcastRef.value
      val index = MyUtil.binarySearch(rulesInExecutor,ipNum)
      var province = "火星"
      if (index != -1){
        province = rulesInExecutor(index)._3
      }
      (province,1)
    }

    val provinceAndOne = lines.map(func)

    val reduced = provinceAndOne.reduceByKey(_+_)

    val r = reduced.collect()
    //reduced.saveAsHadoopFile()
    println(r.toBuffer)

    sc.stop()
  }
}
