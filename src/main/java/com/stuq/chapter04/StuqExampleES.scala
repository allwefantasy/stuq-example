package com.stuq.chapter04

import _root_.kafka.serializer.StringDecoder
import com.stuq.chapter02.Mock
import com.stuq.nginx.parser.NginxParser
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka.KafkaUtils
import org.elasticsearch.spark.rdd.EsSpark

/**
 * 4/25/16 WilliamZhu(allwefantasy@gmail.com)
 */
object StuqExampleES {
  def main(args: Array[String]) = {

    val conf = new SparkConf().setAppName("测试Streaming应用")
    val isDebug = true
    val duration = 5
    if (isDebug) {
      conf.setMaster("local[2]")
    }
    val ssc = new StreamingContext(conf, Seconds(duration))

    val input = if (isDebug) new TestInputStream[String](ssc, Mock.items, 1)
    else {
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc,
        Map("metadata.broker.list" -> "broker1"),
        Set("topic")
      ).map(f => f._2)
    }

    //Transform
    val result = input.map { nginxLogLine =>
      val items = NginxParser.parse(nginxLogLine)
      ("domain",items(2).split("/")(2))
    }

    //这个是存储
    result.foreachRDD { rdd =>
      val cfg = Map(
        "es.resource" -> "test/test",
        "es.nodes" -> "10.75.137.69"
      )
      EsSpark.saveToEs(rdd, cfg)


      val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
      val df = sqlContext.read.format("org.elasticsearch.spark.sql").options(cfg).load("test/test")
      df.registerTempTable("test")

      sqlContext.sql("select * from test").foreach(f=>println(f))

    }

    //我接着再读
    ssc.start()
    ssc.awaitTermination()

  }
}
