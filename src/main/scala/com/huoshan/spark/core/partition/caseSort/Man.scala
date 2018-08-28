package com.huoshan.partition.caseSort

case class Man (age :Int ,fv : Int ) extends Ordered[Man]{
  override def compare(that: Man): Int = {
    if (this.fv == that.fv){
      this.age - that.age
    }else {
      -(this.fv-that.fv)
    }
  }
}
