package com.stuq.chapter02

import net.liftweb.{json => SJSon}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 4/25/16 WilliamZhu(allwefantasy@gmail.com)
 */
object StuqExample3 {
  def main(args: Array[String]) = {

    val conf = new SparkConf().setAppName("测试")

    conf.setMaster("local[4]")
    val sc = new SparkContext(conf)

    val input1 = sc.makeRDD(Seq(("a", 1), ("b", 1)), 2).map { f =>
      Thread.sleep(3000)
      f
    }
    val input2 = sc.makeRDD(Seq(("a", 1), ("a", 1), ("b", 1), ("c", 1), ("d", 1), ("e", 1)), 4).map { f =>
      Thread.sleep(5000)
      f
    }.reduceByKey { (a, b) =>
      Thread.sleep(1000)
      a + b
    }

    val input3 = input1.join(input2, 1)
    input3.count()
    Thread.sleep(30000000)
    sc.stop()

  }
}
