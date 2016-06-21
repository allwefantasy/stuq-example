package com.stuq.chapter03

import com.stuq.chapter02.Mock
import com.stuq.nginx.parser.NginxParser
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.spark.streaming._
import org.apache.spark.{Partitioner, SparkConf}

/**
 * 6/14/16 WilliamZhu(allwefantasy@gmail.com)
 */
object PartionerExample {
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
      (items(2).split("/")(2), nginxLogLine) //(域名,原始日志)
    }

    result.foreachRDD { (rdd, durationTime) =>

      val paths = rdd.map(f => f._1).distinct().collect()

      //[a,b,c] -> zipWithIndex -> [(a,0),(b,1),(c,2)]
      val pathToIndexDis = rdd.context.broadcast(paths.zipWithIndex.toMap)
      val indexToPathDis = rdd.context.broadcast(paths.zipWithIndex.map(_.swap).toMap)

      rdd.partitionBy(new Partitioner {
        override def numPartitions: Int = pathToIndexDis.value.size

        override def getPartition(key: Any): Int = {
          pathToIndexDis.value.get(key.toString).get
        }
      }).mapPartitionsWithIndex((index, data) => {

        // 形成目录
        val dir = s"""file:///tmp/${indexToPathDis.value(index)}/"""
        // 形成文件名
        val fileName = durationTime.milliseconds + "_" + index
        savePartition(dir, fileName, data)
        data
      }, true).count()


    }


    ssc.start()
    ssc.awaitTermination()


  }

  //自定义写HDFS
  def savePartition(path: String, fileName: String, iterator: Iterator[(String, String)]) = {

    var dos: FSDataOutputStream = null

    try {
      val wholePath = path + "/" + fileName
      val fs = FileSystem.get(new Configuration())
      if (!fs.exists(new Path(path))) fs.mkdirs(new Path(path))
      dos = fs.create(new Path(wholePath), true)
      iterator.foreach {
        line =>
          dos.writeBytes(line._2 + "\n")
      }
    } finally {
      if (null != dos) {
        try {
          dos.close()
        } catch {
          case ex: Exception =>
            ex.printStackTrace()
        }
      }
    }


  }
}
