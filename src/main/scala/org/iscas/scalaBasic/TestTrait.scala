package org.iscas.scalaBasic

/**
  * Scala Trait(特征) 相当于 Java 的接口，不同的是，它还可以定义属性和方法的实现。
  */
trait Equal{
  def isEqual(x:Any):Boolean
  def isNotEqual(x:Any):Boolean= !isEqual(x)
}

class Point(xc:Int,yc:Int) extends Equal{
  var x:Int=xc
  var y:Int=yc
  def isEqual(obj:Any)=
    obj.isInstanceOf[Point] && obj.asInstanceOf[Point].x==x
}

object TestTrait {
  def main(args: Array[String]): Unit = {
    val p1=new Point(2,3)
    val p2=new Point(3,4)
    val p3=new Point(2,4)

    println(p1.isEqual(p2))
    println(p1.isNotEqual(p3))
  }
}
