package com.huoshan.spark.graphx

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Description : 逻辑图计算
  * Created by ln on : 2018/8/21 19:13 
  * Author : ln56
  */
object UserTagesMerge {
  def main(args: Array[String]): Unit = {
    //参数个数判断
    if (args.length!=1) {
      println(
        """
          |com.huoshan.spark.graphX.UserTagesMerge
          |
          |Parameter Expect:
          |
          |      inputPath    D:\SparkTest\InputPath\graph.txt
          |
        """.stripMargin
      )
      sys.exit()
    }
    val Array( inputPath ,outputPath ) = args
    //创建spark配置
    val conf = new SparkConf().setAppName(s"${this.getClass.getSimpleName}")
      .setMaster("local[*]")
    //RDD的序列化   worker 和 worker直接的通信
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //Spark的入口
    val sc = new SparkContext(conf)

    val data = sc.textFile(inputPath).map(_.split("\t", -1))

    val uv: RDD[(VertexId, (String, List[(String, Int)]))] = data.flatMap(arr => {
        //区分人名  和   标签     是否包含 " : "
        val userName = arr.filter(!_.contains(":"))
        //val userTages = arr.filter(_.contains(":")).toList

        val userTages: List[(String, Int)] = arr.filter(_.contains(":")).map(kvs => {
          val kv = kvs.split(":")
          (kv(0), kv(1).toInt)
        }).toList
        println(userTages)

        //TODO 构建点   第一个携带  同一行不携带
        userName.map(name => {
          if (name.equals(userName(0))){ //判断人名  是不是第一个   利用userName(0)
            (name.hashCode.toLong, (name, userTages))
          }else{
            (name.hashCode.toLong, (name, List.empty[(String,Int)]))
          }
        })

      })
    val ue = data.flatMap(arr => {
      //区分人名  和   标签
      val userName = arr.filter(!_.contains(":"))
      val userTages = arr.filter(_.contains(":"))
      //TODO 构建边       每一行第一个人名  和   其他人名建立   边
      userName.map(name => Edge(userName(0).hashCode.toLong, name.hashCode.toLong, 0))
    })
    //创建视图
    val graph = Graph(uv,ue)
    val cc = graph.connectedComponents().vertices

    //聚合数据      {}  case 整理数据       这里的数据类型 : ( 0 , ( name ,app ) )
    cc.join(uv).map{    //去重  先用Seq 最后  toSet
      case (id,(cmid,(name,tags))) => (cmid ,(Seq(name) , tags) )
    }.reduceByKey {
      case (t1 , t2) => ({     // foldLeft 数据格式转换
        val k = t1._1 ++ t2._1
        val v= t1._2 ++ t2._2 groupBy(_._1) mapValues(_.foldLeft(0)(_+_._2)) toList

        (k,v)
      })
    }.map(t => (t._2._1.toSet,t._2._2))
     .foreach(println)

    sc.stop()
  }
}
