package com.stuq.chapter05

import com.stuq.chapter02.Mock
import org.apache.spark.SparkConf
import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.streaming.{Seconds, StreamingContext, TestInputStream}

/**
 * 6/29/16 WilliamZhu(allwefantasy@gmail.com)
 */
object KMeansExample {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("测试Streaming应用")
    val isDebug = true
    val duration = 5
    if (isDebug) {
      conf.setMaster("local[2]")
    }
    val ssc = new StreamingContext(conf, Seconds(duration))

    val trainingData = new TestInputStream[String](ssc, Mock.kmeansTraining, 1).map(Vectors.parse)
    val testData = new TestInputStream[String](ssc, Mock.kmeansPredict, 1).map(LabeledPoint.parse)

    val model = new StreamingKMeans()
      .setK(2)
      .setDecayFactor(1.0)
      .setRandomCenters(3, 0.0)

    model.trainOn(trainingData)
    model.predictOnValues(testData.map(lp => (lp.label, lp.features))).print()


    ssc.start()
    ssc.awaitTerminationOrTimeout(30 * 1000)

  }
}
