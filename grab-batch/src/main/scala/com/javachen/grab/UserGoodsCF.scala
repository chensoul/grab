package com.javachen.grab

import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd._
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author <a href="mailto:junechen@163.com">june</a>.
 * @date 2015-05-19 10:18.
 */
object UserGoodsCF {
  def main(args: Array[String]): Unit = {
    //Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    //Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    // set up environment
    val sc = new SparkContext(new SparkConf().setAppName("UserGoodsCF"))
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    sqlContext.parquetFile("/logroot/user_goods_preference").registerTempTable("user_goods_preference")
    val user_goods_preference = sqlContext.sql("SELECT * FROM user_goods_preference where user_id>0")

    //user_goods_preference.collect().foreach(println)

    //0,7,235,1431943216,0,null,null,null,null,null
    val ratings = user_goods_preference.map { t =>
      var score = 0.0
      if (t(2).asInstanceOf[Int] > 0)
        score += 0.7 //访问

      if (t(5).asInstanceOf[Int] > 0)
        score += 0.2 //购买

      if (t(8).asInstanceOf[Float] > 0)
        score += t(8).asInstanceOf[Float] * 0.9 //评分

      if (t(9).asInstanceOf[Int] > 0)
        score += 0.1 //搜藏

      (t(3).asInstanceOf[Int] % 10, Rating(t(0).asInstanceOf[Int], t(1).asInstanceOf[Int], score))
    }

    //ratings.collect().foreach(println)

    val numRatings = ratings.count()
    val numUsers = ratings.map(_._2.user).distinct().count()
    val numGoods = ratings.map(_._2.product).distinct().count()

    println("Got " + numRatings + " ratings from "
      + numUsers + " users on " + numGoods + "goods.")


    val numPartitions = 4
    val training = ratings.values.repartition(numPartitions).cache()

    val rank = 12
    val lambda = 0.1
    val numIter = 20
    val model = ALS.train(training, rank, numIter, lambda)
    val testRmse = computeRmse(model, training, false)

    println("The model was trained with rank = " + rank + " and lambda = " + lambda
      + ", and numIter = " + numIter + ", and its RMSE is " + testRmse + ".")
  }

  /** Compute RMSE (Root Mean Squared Error). */
  def computeRmse(model: MatrixFactorizationModel, data: RDD[Rating], implicitPrefs: Boolean) = {

    def mapPredictedRating(r: Double) = if (implicitPrefs) math.max(math.min(r, 1.0), 0.0) else r

    val predictions: RDD[Rating] = model.predict(data.map(x => (x.user, x.product)))
    val predictionsAndRatings = predictions.map { x =>
      ((x.user, x.product), mapPredictedRating(x.rating))
    }.join(data.map(x => ((x.user, x.product), x.rating))).values
    math.sqrt(predictionsAndRatings.map(x => (x._1 - x._2) * (x._1 - x._2)).mean())
  }

}
