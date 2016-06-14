package com.stuq.chapter03

import _root_.kafka.serializer.StringDecoder
import com.stuq.chapter02.Mock
import com.stuq.nginx.parser.NginxParser
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka.KafkaUtils

/**
 * 4/25/16 WilliamZhu(allwefantasy@gmail.com)
 */
object MapWithStateExample {
  def main(args: Array[String]) = {

    val conf = new SparkConf().setAppName("测试Streaming应用")
    val isDebug = true
    val duration = 5
    if (isDebug) {
      conf.setMaster("local[2]")
    }



    def createContext = {
      val ssc = new StreamingContext(conf, Seconds(duration))
      ssc.checkpoint("file:///tmp/mapwithstate/")


      val input = new TestInputStream[String](ssc, Mock.items2, 1)

      val result = input.map { nginxLogLine =>
        val items = NginxParser.parse(nginxLogLine)
        (items(2).split("/")(2),nginxLogLine)
      }

      //如果域名相同，我们就拼接起来
      def trackStateFunc(batchTime: Time,
                         key: String,
                         value: Option[String],
                         state: State[List[String]]): Option[(String, List[String])] = {
        val sum = state.getOption.getOrElse(List()) ++ List(value.getOrElse(""))
        val output = (key, sum)
        state.update(sum)
        Some(output)
      }

      val stateSpec = StateSpec.function(trackStateFunc _)

      //首先必须是个key-value ,从mapWithState 的名字可以看得出来
      val newResult = result.map(info => (info._1, info._2)).
        mapWithState(stateSpec)
      newResult.checkpoint(Seconds(5))
      val stateSnapshotStream = newResult.stateSnapshots()

      stateSnapshotStream.foreachRDD{rdd=>
        rdd.collect().foreach(f=>println(s"${f._1}=>${f._2.size}"))
      }

      ssc
    }

    val ssc = createContext//StreamingContext.getActiveOrCreate("file:///tmp/mapwithstate/", () => createContext)

    ssc.start()
    ssc.awaitTermination()


  }
}
