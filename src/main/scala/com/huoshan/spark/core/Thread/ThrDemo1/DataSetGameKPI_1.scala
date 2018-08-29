package com.huoshan.Thread.ThrDemo1

import java.text.SimpleDateFormat

import org.apache.spark.{SparkConf, SparkContext}

object DataSetGameKPI_1 {
  def main(args: Array[String]): Unit = {

    val startTime = args(0)
    val endTime = args(1)

    val dataFormat1 = new SimpleDateFormat("yyyy-MM-dd")

    val startDate = dataFormat1.parse(startTime).getTime
    val endData = dataFormat1.parse(endTime).getTime

    val conf = new SparkConf().setAppName("DataSetGameKPI_1").setMaster("local[4]")

    val sc = new SparkContext(conf)

    val lines = sc.textFile(args(2))

    val splited = lines.map(line => line.split("[|]"))

    val fu = new FilterUtilV1 with Serializable

    val filtered = splited.filter(fields => {
      fu.filterByTime(fields, startDate, endData)
    })

    val dnu = filtered.count()

    println(dnu)

    sc.stop()

  }
}
