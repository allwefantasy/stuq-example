package com.stuq.chapter04

import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{ConnectionFactory, HTable, Put}
import org.apache.hadoop.hbase.util.Bytes


/**
 * 6/20/16 WilliamZhu(allwefantasy@gmail.com)
 */
object SimpleHBaseClient {
  private val DEFAULT_ZOOKEEPER_QUORUM = "127.0.0.1:2181"

  private lazy val table = createConnection

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



  private def createConnection = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", DEFAULT_ZOOKEEPER_QUORUM)
    val conn = ConnectionFactory.createConnection(conf)
    conn.getTable(TableName.valueOf("wiki"))
  }
}
