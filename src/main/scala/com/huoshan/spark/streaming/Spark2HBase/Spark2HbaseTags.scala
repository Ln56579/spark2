package com.huoshan.spark.streaming.Spark2HBase

import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Description : 把spark处理的数据   存入到HBase中
  * Created by ln on : 2018/8/26 19:32 
  * Author : ln56
  */
object Spark2HbaseTags {
  def main(args: Array[String]): Unit = {
    //参数个数判断
    if (args.length!=3) {
      println(
        """
          |com.huoshan.spark.streaming.Spark2HBase.Tage.Spark2HbaseTags
          |
          |Parameter Expect:
          |
          |      inputPath      D:\SparkTest\InputPath\teacher.log
          |      hbaseTableName   tags_
          |      列: day
        """.stripMargin
      )
      sys.exit()
    }
    val Array(inputPath,hbaseTableName,day) = args
    //构建sc
    val conf = new SparkConf()
      .setAppName(s"${this.getClass.getSimpleName}")
      .setMaster("local[4]")
    //RDD的序列化   worker 和 worker直接的通信
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //初始化 sc
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    //设置 日志级别
    sc.setLogLevel(logLevel = "INFO")
    //分离sc初始化   和    正式执行代码
    println("\n\n"+("--"*15)+"  sc init success  "+("--"*15)+"\n\n")

    var number = 0
    //TODO  以下代码     val config = ConfigFactory.load()   load.getString()
    //判断HBase的表是否存在   不存在创建
    val configuration = sc.hadoopConfiguration
    configuration.set("hbase.zookeeper.quorum", "wqb,ll,bzp,yc")

    val hbConn = ConnectionFactory.createConnection(configuration)
    val hbAdmin = hbConn.getAdmin

    if (!hbAdmin.tableExists(TableName.valueOf(hbaseTableName))){
      println(s"$hbaseTableName 表不存在...")
      println(s"正在创建 $hbaseTableName")
      //创建表
      val tableDesc = new HTableDescriptor(TableName.valueOf(hbaseTableName))
      val columnDescriptor = new HColumnDescriptor("cf")
      tableDesc.addFamily(columnDescriptor)
      hbAdmin.createTable(tableDesc)

      hbAdmin.close()
      hbConn.close()
    }else {
      println(s"$hbaseTableName 已经存在")
    }

    val jobConf = new JobConf(configuration)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE,hbaseTableName)

    sc.textFile(inputPath).map(
      line => {
        //截取
        val splits: Array[String] = line.split("/")
        val teacher = splits(3)
        val subject = splits(2).split("[.]")(0)
        number += 1
        ( teacher+number, List((subject,1)))
      }).map{
      case (teacher, subAndOne: List[Any]) => {
        //构建Put
         val put = new Put(Bytes.toBytes(teacher))
          //TODO  把数组转换成字符串
        val tags = subAndOne.map(t => t._1+":"+t._2).mkString(",")

        put.addColumn(Bytes.toBytes("cf"),Bytes.toBytes(s"day$day"),Bytes.toBytes(tags))

        (new ImmutableBytesWritable(),put)
      }
    }.saveAsHadoopDataset(jobConf)











  }
}
