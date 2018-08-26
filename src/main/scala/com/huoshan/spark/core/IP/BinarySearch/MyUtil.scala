package com.huoshan.IP.BinarySearch

import scala.io.{BufferedSource, Source}

object MyUtil {
  /**
    * 把ip地址转换成10进制            线程不安全   看有没有共享的成员变量     局部变量没事
    *                                          只读也可以                局部变量的好处
    * @param ip 118.74.247.170
    * @return 十进制的ip
    */
  def ip2Long(ip : String):Long = {
    val fragments = ip.split("[.]")
    var ipNum = 0L
    for(i <- 0 until fragments.length){
      ipNum = fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }

  /**
    * 二分查找 ip
    * @param lines ip规则
    * @param ip 十进制的ip
    * @return ip在数组中的角标
    */
  def binarySearch(lines : Array[(Long,Long,String)],ip:Long):Int = {
    var low = 0
    var high = lines.length-1
    while (low <= high) {
      val middle = (low+high)/2
      if ((ip >= lines(middle)._1) && ( ip <= lines(middle)._2))
        return middle
      if (ip < lines(middle)._1)
        high = middle - 1
      else {
        low = middle + 1
      }
    }
    -1
  }

  /**
    * 加载IP规则
    * @param Path 读取本地文件的路径
    * @return 截取所需要的数据
    */
  def readRules(Path:String):Array[(Long,Long,String)] = {
    val bf: BufferedSource = Source.fromFile(Path)

    val lines = bf.getLines()

    val rules = lines.map(line => {
      val fileds = line.split("[|]")
      val startNum = fileds(2).toLong
      val endNum = fileds(3).toLong
      val province = fileds(6)
      (startNum, endNum, province)
    }).toArray
    rules
  }

  /**
    * test1  测试本地小程序
    */
  def main(args: Array[String]): Unit = {

    val rules = readRules("D:\\ip\\ip.txt")
    val ip = ip2Long("178.74.247.170")
    val rulesNum = binarySearch(rules,ip)

    if (rulesNum != -1) {
      val dizhi = rules(rulesNum)._3
      println(dizhi)
    }else{
      println("火星")
    }
  }
}
