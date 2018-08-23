package com.huoshan.spark.core.teacher.Executors.Partitioner

import org.apache.spark.Partitioner

import scala.collection.mutable

/**
  * Description :  自定义分区类
  * Created by ln on : 2018/8/23 16:48 
  * Author : ln56
  */
class SubjectPartitioner(sbs : Array[String] ) extends Partitioner {

  private val rules = new mutable.HashMap[String,Int]()
  var i = 0             //定义一个map  把学科  和   分区编号放到规则里
  for ( sb <- sbs){
    rules(sb) = i
    i += 1
  }

  //返回分区的数量    (下个RDD有多少个分区)
  override def numPartitions: Int = sbs.length     // 5个分区

  //安 key  进行分区   里面的值是 (string,string)
  override def getPartition(key: Any): Int = {    //返回 0 1 2 3 4
    val subject = key.asInstanceOf[(String,String)]._1
    //根据学科  获取分区数
    rules(subject)
  }
}
