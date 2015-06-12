package com.javachen.grab.spark.cf

import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.jblas.DoubleMatrix

/**
 * Created by Alesis Novik
 * Code snippet for generating bulk recommendations.
 *
 * http://www.alesisnovik.com/?p=8
 */

object BulkRecommender {
  def recommend(sc: SparkContext, model: MatrixFactorizationModel, numberOfCores: Int): RDD[(Int, Array[Rating])] = {
    // Collect product feature matrix
    val productFeatures = model.productFeatures.collect()

    // Build product feature matrix for jblas and broadcast to all nodes.
    var productArray = new Array[Int](0)
    var productFeaturesArray = new Array[Array[Double]](0)
    for ((product, features) <- productFeatures) {
      productArray = productArray :+ product
      productFeaturesArray = productFeaturesArray :+ features
    }

    val productArrayBroadcast = sc.broadcast(productArray)
    val productFeatureMatrixBroadcast =
      sc.broadcast(new DoubleMatrix(productFeaturesArray).transpose())

    val recommendations = model.userFeatures.repartition(numberOfCores * 5).mapPartitions { iter =>
      // Build user feature matrix for jblas
      var userFeaturesArray = new Array[Array[Double]](0)
      var userArray = new Array[Int](0)
      while (iter.hasNext) {
        val (user, features) = iter.next()
        userArray = userArray :+ user
        userFeaturesArray = userFeaturesArray :+ features
      }
      val userFeatureMatrix = new DoubleMatrix(userFeaturesArray)

      // Multiply the user feature and product feature matrices using jblas
      val userRecommendationArray =
        userFeatureMatrix.mmul(productFeatureMatrixBroadcast.value).toArray2
      var mappedUserRecommendationArray = new Array[(Int, Array[Rating])](0)

      // Extract ratings from the matrix
      for (i <- 0 until userArray.length) {
        var ratingArray = new Array[Rating](0)
        for (j <- 0 until productArrayBroadcast.value.length) {
          ratingArray = ratingArray :+ new Rating(userArray(i),
            productArrayBroadcast.value(j), userRecommendationArray(i)(j))
        }
        // Take the top 10 recommendations for the user
        mappedUserRecommendationArray = mappedUserRecommendationArray :+
          (userArray(i), ratingArray.sortBy(-_.rating).take(10))
      }
      mappedUserRecommendationArray.iterator
    } setName "ALSRecs"

    // Collect and return the recommendations
    recommendations
  }
}