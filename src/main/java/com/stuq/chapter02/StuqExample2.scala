package com.stuq.chapter02

import _root_.kafka.serializer.StringDecoder
import com.stuq.nginx.parser.NginxParser
import net.liftweb.{json => SJSon}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka.KafkaUtils
import org.elasticsearch.spark._

import scala.collection.mutable.{ArrayBuffer, SynchronizedBuffer}

/**
 * 4/25/16 WilliamZhu(allwefantasy@gmail.com)
 */
object StuqExample2 {
  def main(args: Array[String]) = {

    val conf = new SparkConf().setAppName("测试Streaming应用")

    conf.set("es.nodes", "127.0.0.1")
    conf.set("es.resource", "test/test")

    val isDebug = true
    val duration = 5
    if (isDebug) {
      conf.setMaster("local[2]")
    }
    val ssc = new StreamingContext(conf, Seconds(duration))
    val batchCount = new BatchCounter(ssc)

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
      items(2).split("/")(2)
    }.map { f =>
      implicit val formats = SJSon.Serialization.formats(SJSon.NoTypeHints)
      SJSon.Serialization.write(Map("domain" -> f))
    }


    if (isDebug) {
      val outputBuffer = new ArrayBuffer[Seq[String]] with SynchronizedBuffer[Seq[String]]
      val outputStream = new TestOutputStream(result, outputBuffer)
      outputStream.registerMe()
      ssc.start()

      batchCount.waitUntilBatchesCompleted(3, 50 * 1000)

      assert(outputStream.output(0).mkString("") == "abc")
      assert(outputStream.output(1).mkString("") == "bbc")
      assert(outputStream.output(2).mkString("") != "ccd")

      ssc.awaitTerminationOrTimeout(1)
      ssc.stop()
    } else {

      result.foreachRDD { rdd =>
        rdd.foreachPartition(line => println(line))
      }

      result.foreachRDD { rdd =>
        rdd.saveJsonToEs("test/test")
      }

      ssc.start()
      ssc.awaitTermination()

    }


  }
}
