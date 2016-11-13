package com.stuq.chapter05

import com.stuq.chapter02.Mock
import org.apache.spark.ml.clustering.{KMeans, KMeansModelPerJVM}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.{Row, DataFrame, SQLContext}
import org.apache.spark.streaming.{Seconds, StreamingContext, TestInputStream}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 6/29/16 WilliamZhu(allwefantasy@gmail.com)
 */
object KMeansExample2 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("测试Streaming应用")
    val isDebug = true
    val duration = 5
    if (isDebug) {
      conf.setMaster("local[2]")
    }
    val ssc = new StreamingContext(conf, Seconds(duration))

    val testData = new TestInputStream[String](ssc, Mock.kmeansPredict, 1).map(LabeledPoint.parse)
    testData.foreachRDD { rdd =>
      val sqlContext = SQLContext.getOrCreate(rdd.context)
      val df: DataFrame = sqlContext.createDataFrame(rdd).toDF("id", "features")

      import org.apache.spark.mllib.linalg.Vector

      val predictDataFrame = df.map { k:Row =>
        (k.getAs[Double]("id"), KMeansModelPerJVM.predict(k.getAs[Vector]("features")))
      }

      predictDataFrame.foreach { f =>
        println(s"${f._1} ${f._2} ")
      }

    }


    ssc.start()
    ssc.awaitTermination()

  }
}

object KMeansExampleSaveModel {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("测试Streaming应用")
    val isDebug = true
    val duration = 5
    if (isDebug) {
      conf.setMaster("local[2]")
    }
    val sc = new SparkContext(conf)
    val sqlContext = SQLContext.getOrCreate(sc)

    val dataset: DataFrame = sqlContext.createDataFrame(Seq(
      (1, Vectors.dense(0.0, 0.0, 0.0)),
      (2, Vectors.dense(0.1, 0.1, 0.1)),
      (3, Vectors.dense(0.2, 0.2, 0.2)),
      (4, Vectors.dense(9.0, 9.0, 9.0)),
      (5, Vectors.dense(9.1, 9.1, 9.1)),
      (6, Vectors.dense(9.2, 9.2, 9.2))
    )).toDF("id", "features")


    val kmeans = new KMeans()
      .setK(2)
      .setFeaturesCol("features")
      .setPredictionCol("prediction")
    val model = kmeans.fit(dataset)

    model.save("/tmp/kmeans")

    sc.stop()

  }
}
