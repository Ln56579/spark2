package com.huoshan.input.Hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}

object baseRDDDemo {
  def main(args: Array[String]): Unit = {

    val tablename = args(0)

    val sparkConf = new SparkConf().setAppName("baseRDDDemo").setMaster("local[4]")

    val sc = new SparkContext(sparkConf)

    val conf = HBaseConfiguration.create()
    conf.set("hbase.rootdir", "hdfs://ln1:9000/hbase")
    conf.set("hbase.zookeeper.quorum", "ln1,ln2,ln3")
    conf.set(TableInputFormat.INPUT_TABLE, tablename)

    val hBaseRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(conf,
      classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    //创建RDD,这个RDD会记录以后从mysql中的数据     RDD记录从哪里都数据  没有真正的数据

    val hbaseDemo = hBaseRDD.count()
    println(hbaseDemo)

    hBaseRDD.foreach(rs =>{
      val key: String = Bytes.toString(rs._2.getRow)
      val city = Bytes.toString(rs._2.getValue("cf".getBytes(),"city".getBytes()))
      val count = Bytes.toString(rs._2.getValue("cf".getBytes(),"count".getBytes()))
      println(s"key :$key  + city : $city + count : $count")
    })

    sc.stop()
  }
}
