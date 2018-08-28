package com.huoshan.output.Mysql

import java.sql.DriverManager

import com.huoshan.IP.BinarySearch.MyUtil
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object PutMySQL {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("PutMySQL").setMaster("local[4]")

    val sc = new SparkContext(conf)
    //在driver端获取所有的IP规则然后广播出去
    val rules: Array[(Long, Long, String)] = MyUtil.readRules(args(0))
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

//    val r = reduced.collect()
//    println(r.toBuffer)
    //action  没有返回值

    reduced.foreachPartition( it => {
      //将数据写入到数据库中       这是在Executor中连接的
      val conn = DriverManager.getConnection("jdbc:mysql://ln1:3306/ln?charatorEncoding=utf-8","root","Xm123456@")
      val pstm = conn.prepareStatement("insert into ACCESS_LOG values(?,?)")
      it.foreach(tp => {
        pstm.setString(1,tp._1)
        pstm.setInt(2,tp._2)
        pstm.executeUpdate()
      })
      pstm.close()
      conn.close()
    })

    sc.stop()
  }
}
