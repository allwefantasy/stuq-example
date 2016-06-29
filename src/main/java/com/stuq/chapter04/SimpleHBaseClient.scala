package com.stuq.chapter04

import org.apache.hadoop.hbase.client.{ConnectionFactory, Get, HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}



/**
 * 6/20/16 WilliamZhu(allwefantasy@gmail.com)
 */
object SimpleHBaseClient {
  private val DEFAULT_ZOOKEEPER_QUORUM = "127.0.0.1:2181"

  private lazy val (table, conn) = createConnection

  def createTableIfNotExists(tn: String) = {
    val tableName = TableName.valueOf(tn)
    if (!conn.getAdmin.tableExists(tableName)) {
      val tableDescriptor = new HTableDescriptor(tableName)
      val hcd = new HColumnDescriptor("v")
      tableDescriptor.addFamily(hcd)
      conn.getAdmin.createTable(tableDescriptor)
    }
  }

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


  def get(rowKey: String) = {
    val get = new Get(Bytes.toBytes(rowKey))
    try {
      val result = table.get(get)
      Bytes.toString(result.getValue(Bytes.toBytes("v"), Bytes.toBytes("c")))
    }
    catch {
      case e: Exception =>
        e.printStackTrace()
        ""
    }
  }


  private def createConnection = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", DEFAULT_ZOOKEEPER_QUORUM)
    val conn = ConnectionFactory.createConnection(conf)
    (conn.getTable(TableName.valueOf("wiki")), conn)
  }
}
