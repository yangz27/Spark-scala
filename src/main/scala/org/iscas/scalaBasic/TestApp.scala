package org.iscas.scalaBasic

/**
  * App特质的作用就是延迟初始化,从代码上看它继承自DelayedIni
  * 发者可以直接在初始化块里写逻辑
  * 然后编译器会把这段初始化代码块里的逻辑封装成一个函数对象(是(() => Unit)类型)
  * 缓存起来(并没有运行)，然后放到一个集合(ListBuffer)中，之后在main方法里一行一行调用并执行，
  * 所以只有在执行到main方法的时候才会触发，从而达到延迟初始化的效果。
  */
object TestApp extends App {
  println("testing app")
}
