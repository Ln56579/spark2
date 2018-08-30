package com.sql.spark_2.join.IPSQL.Demo

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object IPLocaltionSQL {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("IPLocaltionSQL").master("local[*]").getOrCreate()
    import spark.implicits._
    val rules: Dataset[String] = spark.read.textFile(args(0))
    //在driver端获取所有的IP规则然后广播出去
    val rulesDataFrame: DataFrame = rules.map(line => {
      val fileds = line.split("[|]")
      val startNum = fileds(2).toLong
      val endNum = fileds(3).toLong
      val province = fileds(6)
      (startNum, endNum, province)
    }).toDF("snum", "enum", "province")


    val lines: Dataset[String] = spark.read.textFile(args(1))

    val ips = lines.map(line => {
      val fieds = line.split("[|]")
      val ip = fieds(1)
      //将ip转换成10进制的数
      val ipNum = MyUtil1.ip2Long(ip)
      ipNum
    }).toDF("ip_num")

    rulesDataFrame.createTempView("v_rules")
    ips.createTempView("v_ips")

    val r = spark.sql("select province,count(*) counts from v_ips join v_rules on (ip_num >= snum and ip_num <= enum) group by province order by counts desc")

    r.show()

    spark.stop()
  }
}
