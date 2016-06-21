package com.stuq.chapter02

import _root_.kafka.serializer.StringDecoder
import com.stuq.nginx.parser.NginxParser
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka.KafkaUtils

/**
 * 4/25/16 WilliamZhu(allwefantasy@gmail.com)
 */
object StuqExampleMapWithState {
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


      val input = if (isDebug) new TestInputStream[String](ssc, Mock.items, 1)
      else {
        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc,
          Map("metadata.broker.list" -> "broker1"),
          Set("topic")
        ).map(f => f._2)
      }


      val result = input.map { nginxLogLine =>
        val items = NginxParser.parse(nginxLogLine)
        items(2).split("/")(2)
      }


      /*
          参看: https://docs.cloud.databricks.com/docs/spark/1.6/examples/Streaming%20mapWithState.html
          这里的key,value,state 可以是三个不同用户自定义对象，不一定是基本对象或者仅仅做累加，一个更复杂的例子如：

          def stateUpdateFunction(
                     userId: UserId,
                     newData: UserAction,
                     stateData: State[UserSession]): UserModel = {

                     val currentSession = stateData.get()    // Get current session data
                     val updatedSession = ...            // Compute updated session using newData
                     stateData.update(updatedSession)            // Update session data

                     val userModel = ...                 // Compute model using updatedSession
                     return userModel                // Send model downstream
}
         */
      def trackStateFunc(batchTime: Time,
                         key: String,
                         value: Option[Int],
                         state: State[Long]): Option[(String, Long)] = {
        val sum = value.getOrElse(0).toLong + state.getOption.getOrElse(0L)
        val output = (key, sum)
        state.update(sum)
        Some(output)
      }

      val initialRDD = ssc.sparkContext.parallelize(List(("abc", 100L), ("bbc", 32L)))
      val stateSpec = StateSpec.function(trackStateFunc _).initialState(initialRDD)
        .numPartitions(2)
        .timeout(Seconds(60))


      //首先必须是个key-value ,从mapWithState 的名字可以看得出来
      val newResult = result.map(domain => (domain, 1)).mapWithState(stateSpec)
      newResult.checkpoint(Seconds(5))
      val stateSnapshotStream = newResult.stateSnapshots()

      stateSnapshotStream.foreachRDD { rdd =>
        rdd.foreach(k => println(s"计数:${k._1} -> ${k._2}"))
      }

      result.foreachRDD { rdd =>
        rdd.foreach(line => println(line))
      }

      ssc
    }

    val ssc = StreamingContext.getActiveOrCreate("file:///tmp/mapwithstate/", () => createContext)

    ssc.start()
    ssc.awaitTermination()


  }
}
