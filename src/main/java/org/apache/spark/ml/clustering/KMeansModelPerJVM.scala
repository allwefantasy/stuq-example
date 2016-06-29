package org.apache.spark.ml.clustering

import org.apache.spark.mllib.linalg.Vector

/**
 * 6/29/16 WilliamZhu(allwefantasy@gmail.com)
 */
object KMeansModelPerJVM {
  private val model = KMeansModel.load("/tmp/kmeans")

  def predict(vector: Vector) = {
    val parentModel = classOf[KMeansModel].getDeclaredField("parentModel")
    parentModel.setAccessible(true)
    parentModel.get(model).asInstanceOf[org.apache.spark.mllib.clustering.KMeansModel].predict(vector)
  }
}
