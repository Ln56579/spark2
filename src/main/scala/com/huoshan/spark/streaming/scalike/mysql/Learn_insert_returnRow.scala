package com.huoshan.scalike.mysql

import scalikejdbc._
import scalikejdbc.config.DBs

/**
  * Description : TODO
  * Created by ln on : 2018/8/17 16:02 
  * Author : nan
  */
object Learn_insert_returnRow {
  def main(args: Array[String]): Unit = {
    DBs.setup()
    val insertResult: Long = DB.localTx{ implicit session =>
      SQL("insert into test(id,name,age) value(?,?,?) ").bind("8", "acca", "31").updateAndReturnGeneratedKey("id").apply()
    }
    print(insertResult)
  }
}
