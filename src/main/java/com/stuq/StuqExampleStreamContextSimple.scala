package com.stuq

import _root_.kafka.serializer.StringDecoder
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 4/25/16 WilliamZhu(allwefantasy@gmail.com)
 */
object StuqExampleStreamContextSimple {
  def main(args: Array[String]) = {

    val conf = new SparkConf().setAppName("测试Streaming应用")
    conf.setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))

    val input = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc,
      Map("metadata.broker.list" -> "broker1"),
      Set("topic")
    ).map(f => f._2)

    input.map(f => (f, 1)).
      reduceByKey((a, b) => a + b).
      saveAsTextFiles("your path")

    ssc.start()
    ssc.awaitTermination()
  }
}

object StuqExampleSimple {
  def main(args: Array[String]) = {

    val conf = new SparkConf().setAppName("测试")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)

    val input1 = sc.textFile("....").map(f => (f, 1))
    val input2 = sc.objectFile[(String, Int)]("...")

    val input3 = input1.join(input2)
    input3.count()
    sc.stop()
  }
}
