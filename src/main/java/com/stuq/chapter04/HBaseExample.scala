package com.stuq.chapter04

import com.stuq.chapter02.Mock
import com.stuq.nginx.parser.NginxParser
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext, TestInputStream}

/**
 * 6/20/16 WilliamZhu(allwefantasy@gmail.com)
 */
object HBaseExample {
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

    result.foreachRDD { rdd =>
      rdd.foreachPartition { f =>
        SimpleHBaseClient.createTableIfNotExists("wiki")
        f.foreach { line =>
          //RowKey规则: md5+";"+datetime+";"+realkey
          val key = System.currentTimeMillis() + ""
          SimpleHBaseClient.put(key, line)
          println(SimpleHBaseClient.get(key))
        }
      }
    }

    ssc.start()
    ssc.awaitTermination()


  }
}
