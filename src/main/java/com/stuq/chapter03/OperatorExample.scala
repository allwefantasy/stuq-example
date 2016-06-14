package com.stuq.chapter03

import com.stuq.chapter02.Mock
import com.stuq.nginx.parser.NginxParser
import org.apache.spark.SparkConf
import org.apache.spark.streaming._

/**
 * 6/14/16 WilliamZhu(allwefantasy@gmail.com)
 */
object OperatorExample {
  def main(args: Array[String]) = {

    val conf = new SparkConf().setAppName("测试Streaming应用")

    val isDebug = true
    val duration = 5
    if (isDebug) {
      conf.setMaster("local[2]")
    }
    val ssc = new StreamingContext(conf, Seconds(duration))

    val input = new TestInputStream[String](ssc, Mock.items2, 1)


    val result = input.map { nginxLogLine =>
      val items = NginxParser.parse(nginxLogLine)
      items(2).split("/")(2)
    }

    //类似countByValue的效果
    result.map(f => (f, 1)).reduceByKey((a, b) => a + b).print
    //类似groupBy的效果
    result.map(f => (f, List(1))).reduceByKey((a, b) => a ++ b).print()

    result.countByValue().print()

    result.window(Seconds(5*3)).map(f => (f, List(1))).reduceByKey((a, b) => a ++ b).print()


    ssc.start()
    ssc.awaitTermination()


  }
}
