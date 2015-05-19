package com.javachen.grab

import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd._
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author <a href="mailto:junechen@163.com">june</a>.
 * @date 2015-05-19 10:18.
 */
object UserGoodsCFTest {
  def main(args: Array[String]): Unit = {
    //Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    //Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    // set up environment
    val sc = new SparkContext(new SparkConf().setAppName("UserGoodsCFTest"))
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    sqlContext.parquetFile("/logroot/user_goods_preference").registerTempTable("user_goods_preference")
    val user_goods_preference = sqlContext.sql("SELECT * FROM user_goods_preference where user_id>0 limit 10")

    //user_goods_preference.map(t=>(t(0),t(1))).saveAsTextFile("/tmp/user_goods_preference")

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

    println("Got " + numRatings + " ratings from " + numUsers + " users on " + numGoods + "goods.")

    // split ratings into train (60%), validation (20%), and test (20%) based on the
    // last digit of the timestamp, add myRatings to train, and cache them

    val numPartitions = 4
    val training = ratings.filter(x => x._1 < 6).values.repartition(numPartitions).cache()
    val validation = ratings.filter(x => x._1 >= 6 && x._1 < 8).values.repartition(numPartitions).cache()
    val test = ratings.filter(x => x._1 >= 8).values.cache()

    val numTraining = training.count()
    val numValidation = validation.count()
    val numTest = test.count()

    println("Training: " + numTraining + ", validation: " + numValidation + ", test: " + numTest)

    // train models and evaluate them on the validation set
    val ranks = List(8, 12)
    val lambdas = List(0.1, 10.0)
    val numIterations = List(10, 20)
    var bestModel: Option[MatrixFactorizationModel] = None
    var bestValidationRmse = Double.MaxValue
    var bestRank = 0
    var bestLambda = -1.0
    var bestNumIter = -1
    for (rank <- ranks; lambda <- lambdas; numIter <- numIterations) {
      val model = ALS.train(training, rank, numIter, lambda)
      val validationRmse = computeRmse(model, validation)
      println("RMSE (validation) = " + validationRmse + " for the model trained with rank = "
        + rank + ", lambda = " + lambda + ", and numIter = " + numIter + ".")
      if (validationRmse < bestValidationRmse) {
        bestModel = Some(model)
        bestValidationRmse = validationRmse
        bestRank = rank
        bestLambda = lambda
        bestNumIter = numIter
      }
    }

    // evaluate the best model on the test set
    val testRmse = computeRmse(bestModel.get, test)

    println("The best model was trained with rank = " + bestRank + " and lambda = " + bestLambda
      + ", and numIter = " + bestNumIter + ", and its RMSE on the test set is " + testRmse + ".")

    // create a naive baseline and compare it with the best model

    val meanRating = training.union(validation).map(_.rating).mean
    val baselineRmse =
      math.sqrt(test.map(x => (meanRating - x.rating) * (meanRating - x.rating)).mean)
    val improvement = (baselineRmse - testRmse) / baselineRmse * 100
    println("The best model improves the baseline by " + "%1.2f".format(improvement) + "%.")
  }

  /** Compute RMSE (Root Mean Squared Error). */
  def computeRmse(model: MatrixFactorizationModel, data: RDD[Rating]) = {
    val usersProducts = data.map { case Rating(user, product, rate) =>
      (user, product)
    }

    val predictions = model.predict(usersProducts).map { case Rating(user, product, rate) =>
      ((user, product), rate)
    }

    val ratesAndPreds = data.map { case Rating(user, product, rate) =>
      ((user, product), rate)
    }.join(predictions).sortByKey()

    math.sqrt(ratesAndPreds.map { case ((user, product), (r1, r2)) =>
      val err = (r1 - r2)
      err * err
    }.mean())
  }

}
