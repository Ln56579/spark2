package com.huoshan.scalike.mysql

import scalikejdbc.config.DBs
import scalikejdbc._
/**
  * Description : TODO
  * Created by ln on : 2018/8/17 15:52 
  * Author : nan
  */
object Learn_localTx {
  def main(args: Array[String]): Unit = {
    DBs.setup()

    val tx: Int = DB.localTx { implicit session =>
      SQL("insert into test(id,name,age) value(?,?,?) ").bind("7", "五兄", "31").update().apply()
      val i = 1 / 0
      SQL("insert into test(id,name,age) value(?,?,?) ").bind("6", "四弟", "35").update().apply()
    }
    printf(s"tx = $tx")
  }
}
