package com.huoshan.spark.streaming.cms

import com.typesafe.config.ConfigFactory
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils}
import org.apache.spark.streaming.{Duration, StreamingContext}
import scalikejdbc._
import scalikejdbc.config.DBs

/**
  * Description : Streaming  cms
  * Created by ln on : 2018/8/17 17:04
  * Author : nan
  */
object RTMonitor {
  def main(args: Array[String]): Unit = {
    // 1. 加载配置文件
    val config = ConfigFactory.load("application")

    val topicSet = config.getString("kafka.topics").split(",").toSet
    val brokerList = config.getString("kafka.brokerList")
    val group = config.getString("kafka.group")

    println("-----------------" + topicSet)
    println("-----------------" + brokerList)
    println("-----------------" + group)

    // 1. 创建kafka相关参数
    val kafkaParams = Map(
      "metadata.broker.list" -> brokerList,
      "group.id" -> group,
      "auto.offset.reset" -> kafka.api.OffsetRequest.SmallestTimeString
    )

    // 2. 创建SparkStreaming

    val conf = new SparkConf()
      .setAppName(s"${this.getClass.getSimpleName}")
      .setMaster("local[4]")

    val ssc = new StreamingContext(conf, Duration(5000))

    //var fromOffsets: Map[TopicAndPartition, Long] = Map()

    // 3. TODO 从mysql里面找
    DBs.setup()
    //TODO 这里存在一个BUG 当更换主题时需要清除表中数据
    var tmpFromOffsets = DB readOnly { implicit session =>
      sql"select * from stream_offset_24 where groupid = ?".bind(group).map(rs => {
        (TopicAndPartition(rs.string("topic"), rs.int("partition")), rs.long("offset"))
      }).list().apply()
    }
    val fromOffsets: Map[TopicAndPartition, Long] = tmpFromOffsets.toMap

    // 3. 从kafka拉取数据     ---      之前读取mysql中的偏移量
    //假设程序第一次启动
    var stream: InputDStream[(String, String)] = if (fromOffsets.size == 0) {
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicSet)
    } else {
      //程序非第一次启动  从mysql中读取偏移量
      var checedOffset = Map[TopicAndPartition, Long]()
      //TODO 校验偏移量               连接kafka集群连接对象
      val kafkaCluster = new KafkaCluster(kafkaParams)
      val earliesOff1sets = kafkaCluster.getEarliestLeaderOffsets(fromOffsets.keySet)
      //拿每个分区最早的偏移量
      if (earliesOff1sets.isRight) {
        val topAndPartitionOffSet = earliesOff1sets.right.get
        //开始对比      一条一条对比
        checedOffset = fromOffsets.map(owner => {
          val clusterEarliesOffset = topAndPartitionOffSet.get(owner._1).get.offset
          if (owner._2 >= clusterEarliesOffset) {
            owner
          } else {
            (owner._1, clusterEarliesOffset)
          }
        })
      }
      println()
      println("**************")
      println(fromOffsets)
      //kafka拉取的数据     封装成了 messageHandler    (原数据  带描述数据  比如说  在那个分区)
      val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.key(), mmd.message())

      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)
    }
    // 4. 处理数据 根据需求

    stream.foreachRDD(rdd => {
      //TODO   业务逻辑
      // 5. 结果存入到redis
      rdd.foreach(println)


      //TODO 将偏移量存入到mysql
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      offsetRanges.foreach(osr => {
        DB.autoCommit { implicit session =>
          sql"REPLACE INTO stream_offset_24(topic,groupid,partition,offset) VALUE(?,?,?,?)"
            .bind(osr.topic,group,osr.partition,osr.untilOffset).update().apply()
        }
       // println(s"${osr.topic}  ${osr.partition}  ${osr.fromOffset}  ${osr.untilOffset}")
      })
    })

    // 6. 启动程序    ---    等待程序终止
    ssc.start()
    ssc.awaitTermination()
  }
}
