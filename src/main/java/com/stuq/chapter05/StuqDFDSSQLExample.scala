package com.stuq.chapter05

import _root_.kafka.serializer.StringDecoder
import com.stuq.chapter02.Mock
import com.stuq.nginx.parser.NginxParser
import net.liftweb.{json=>SJSon}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka.KafkaUtils


/**
 * 4/25/16 WilliamZhu(allwefantasy@gmail.com)
 */
object StuqDFDSSQLExample {
  def main(args: Array[String]) = {

    val conf = new SparkConf().setAppName("测试Streaming应用")
    val isDebug = true
    val duration = 5
    if (isDebug) {
      conf.setMaster("local[2]")
    }
    val ssc = new StreamingContext(conf, Seconds(duration))

    val input = if (isDebug) new TestInputStream[String](ssc, Mock.items2, 1)
    else {
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc,
        Map("metadata.broker.list" -> "broker1"),
        Set("topic")
      ).map(f => f._2)
    }

    //Transform
    val result = input.map { nginxLogLine =>
      val items = NginxParser.parse(nginxLogLine)
      Map("domain" -> items(2).split("/")(2), "count" -> 1)
    }



    val result2 = result.map(f => DC(f("domain").asInstanceOf[String], f("count").asInstanceOf[Int]))

//    val result3 = result.map{f=>
//      implicit val formats = SJSon.Serialization.formats(SJSon.NoTypeHints)
//      SJSon.Serialization.write(f)
//    }

    //dataset 的例子,不过似乎用的人还是很少
    result2.foreachRDD { rdd =>
      val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
      import org.apache.spark.sql.functions._
      import sqlContext.implicits._

      //case class DC2(domain: String, count: Int)
      val ds = sqlContext.createDataset(rdd).as[DC]
      println("-----ds1 demo--------")
      //select sum(count) as kk from abc group by domain
      ds.groupBy(_.domain).agg(sum("count").as("kk").as[Long]).select(expr("kk").as[Long]).show(10)
      println("-----ds2 demo--------")
      ds.select(expr("count as kk1").as[Int],expr("domain as domain").as[String]).select(expr("kk1").as[Int]).show()
      ds.select(expr("count as kk1").as[Int],expr("domain as kk2").as[String]).show()

    }

    //dataframe 的例子
    result2.foreachRDD { rdd =>
      val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
      val df = sqlContext.createDataFrame(rdd)
      import org.apache.spark.sql.functions._
      println("-----df demo--------")
      //select sum(count) as c2 from table where count > 3 group by domain
      df.where("count > 3").groupBy("domain").agg(sum("count") as "c2").show()
    }

    //sql 的例子
    result2.foreachRDD { rdd =>
      val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
      val df = sqlContext.createDataFrame(rdd)
      df.select("domain","count").registerTempTable("test")
      sqlContext.table("test").printSchema()
      sqlContext.sql("select sum(count) from test group by domain ").show()
    }



  //我接着再读
  ssc.start()
  ssc.awaitTerminationOrTimeout(30*1000)

}

}
case class DC(domain: String, count: Int)