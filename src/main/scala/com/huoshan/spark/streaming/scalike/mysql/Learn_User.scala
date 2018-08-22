package com.huoshan.scalike.mysql

import scalikejdbc._
import scalikejdbc.config.DBs

/**
  * Description : 多列读取 然后封装对象
  * Created by ln on : 2018/8/17 15:36 
  * Author : nan
  */
object Learn_User {
  def main(args: Array[String]): Unit = {
    DBs.setup()
    val users: List[Users] = DB readOnly { implicit session =>
      sql"select * from test".map(rs => Users(rs.int("id"),rs.string("name"),rs.int("age"))).list().apply()
    }
    for(user <- users){
      println(user.id+"  "+user.name+"  "+user.nickName)
    }
  }
}
case class Users(id : Int, name : String , nickName : Int)