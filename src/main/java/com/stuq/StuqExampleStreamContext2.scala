package com.stuq

import com.stuq.nginx.parser.NginxParser
import org.apache.spark.SparkConf
import org.apache.spark.streaming._

/**
 * 4/25/16 WilliamZhu(allwefantasy@gmail.com)
 */
object StuqExampleStreamContext2 {
  def main(args: Array[String]):Unit = {

    val conf = new SparkConf().setAppName("测试Streaming应用")
    val isDebug = true
    val duration = 5
    if (isDebug) {
      conf.setMaster("local[2]")
    }


    val ssc = new StreamingContext(conf, Seconds(duration))

    val item = "2016-02-29/09:55:00 GET \"https://abc/abc/201503/08/23/52/5b1da331305689a3781a181a999458cd_25134600/thumb/2_400_300.jpg\" 200 12 553 30583 TCP_MEM_HIT \"27.43.162.89\" - \"http://m.letv.com/vplay_21344561.html?type=0&id=21344561&ref=baofengyd\" \"image/jpeg\" \"Mozilla/5.0 (Linux; U; Android 4.1.1; zh-cn; MI 2S Build/JRO03L) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 Mobile Safari/534.30 storm_browser\" 553 200 *Not IP address [0]*"
    val item2 = "2016-02-29/09:55:00 GET \"https://bbc/abc/201503/08/23/52/5b1da331305689a3781a181a999458cd_25134600/thumb/2_400_300.jpg\" 200 12 553 30583 TCP_MEM_HIT \"27.43.162.89\" - \"http://m.letv.com/vplay_21344561.html?type=0&id=21344561&ref=baofengyd\" \"image/jpeg\" \"Mozilla/5.0 (Linux; U; Android 4.1.1; zh-cn; MI 2S Build/JRO03L) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 Mobile Safari/534.30 storm_browser\" 553 200 *Not IP address [0]*"
    val item3 = "2016-02-29/09:55:00 GET \"https://ccd/abc/201503/08/23/52/5b1da331305689a3781a181a999458cd_25134600/thumb/2_400_300.jpg\" 200 12 553 30583 TCP_MEM_HIT \"27.43.162.89\" - \"http://m.letv.com/vplay_21344561.html?type=0&id=21344561&ref=baofengyd\" \"image/jpeg\" \"Mozilla/5.0 (Linux; U; Android 4.1.1; zh-cn; MI 2S Build/JRO03L) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 Mobile Safari/534.30 storm_browser\" 553 200 *Not IP address [0]*"

    val input = new TestInputStream[String](ssc, Seq(Seq(item), Seq(item2), Seq(item3)), 1)

    val result = input.map { nginxLogLine =>
      val items = NginxParser.parse(nginxLogLine)
      items(2).split("/")(2)
    }

    result.foreachRDD { rdd =>
      rdd.foreachPartition(lines => lines.foreach(println _))
    }


    ssc.start()
    ssc.awaitTermination()


  }
}
