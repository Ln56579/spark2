package com.huoshan.scalike.mysql
import scalikejdbc._
import scalikejdbc.config.DBs

/**
  * Description : 单条插入数据
  * Created by ln on : 2018/8/17 15:46 
  * Author : nan
  */
object Learn_insert {
  def main(args: Array[String]): Unit = {
    DBs.setup()
    val insertResult = DB.autoCommit { implicit session =>
      SQL("insert into test(id,name,age) value(?,?,?) ").bind("4", "joda", "29").update().apply()
    }
    print(insertResult)

    //更新数据
    val tx: Int = DB.localTx { implicit session =>
      SQL("update test set id =? ").bind("7").update().apply()
    }
  }
}
