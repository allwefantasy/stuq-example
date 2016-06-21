package com.stuq.chapter04

/**
 * 6/21/16 WilliamZhu(allwefantasy@gmail.com)
 */
object ScalaExample {
  def main(args: Array[String]): Unit = {
    val a = new A()
    println(a.b)
  }
}

class A {
  def a = {
    "A.a"
  }
}

class B(a: A) {
  def b = {
    "B.b"
  }
}

object TaskServiceE {
  implicit def mapAToB(a: A): B = {
    new B(a)
  }
}
