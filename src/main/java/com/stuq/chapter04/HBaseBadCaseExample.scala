package com.stuq.chapter04

import com.stuq.chapter02.Mock
import com.stuq.nginx.parser.NginxParser
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{HTable, Put, ConnectionFactory}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext, TestInputStream}

/**
 * 6/20/16 WilliamZhu(allwefantasy@gmail.com)
 */
object HBaseBadCaseExample {
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
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", "127.0.0.1:2181")
    val conn = ConnectionFactory.createConnection(hbaseConf)
    val table = conn.getTable(TableName.valueOf("wiki"))


    def put(rowKey: String, value: String) = {
      val put = new Put(Bytes.toBytes(rowKey))
      put.addColumn(Bytes.toBytes("v"), Bytes.toBytes("c"), Bytes.toBytes(value))
      try {
        table.put(put)
        table.asInstanceOf[HTable].flushCommits()
      }
      catch {
        case e: Exception =>
          e.printStackTrace()
      }
    }

    result.foreachRDD { rdd =>
      rdd.foreachPartition { f =>
        f.foreach { line =>
          //RowKey规则: md5+";"+datetime+";"+realkey
          put(System.currentTimeMillis() + "", line)
        }
      }
    }

    ssc.start()
    ssc.awaitTermination()


  }
}
