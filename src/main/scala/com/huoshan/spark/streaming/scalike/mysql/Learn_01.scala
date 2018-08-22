package com.huoshan.scalike.mysql

import scalikejdbc.config.DBs
import scalikejdbc._
/**
  * Description : ScaLikeJdbc 测试连接代码
  * Created by ln on : 2018/8/17 14:59 
  * Author : nan
  */
object Learn_01 {

  def main(args: Array[String]): Unit = {
    //加载配置文件 db.default.*
    DBs.setup()
    //readOnly 只读方式
    val list: List[String] = DB readOnly { implicit session =>
      sql"select * from test".map(rs => rs.string("name")).list().apply()
    }
    for (s <- list){
      println(s)
    }

  }

}
