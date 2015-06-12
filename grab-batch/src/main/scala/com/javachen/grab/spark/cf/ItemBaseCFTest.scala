package com.javachen.grab.spark.cf

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable


/**
 * 基于物品的协同过滤，根据不同输入参数计算最佳训练模型
 *
 * @author <a href="mailto:junechen@163.com">june</a>.
 *         2015-05-19 10:18
 */
object ItemBaseCFTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ItemBaseCFTest")
    conf.registerKryoClasses(Array(classOf[mutable.BitSet], classOf[Rating])).set("spark.kryoserializer.buffer.mb", "100")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.akka.frameSize", "50"); //单位是MB

    val sc = new SparkContext(conf)

    val hc = new HiveContext(sc)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val data = hc.sql("select * from dw_rec.user_goods_preference").cache

    val ratings = data.map { t =>
      var score = 0.0
      if (t(4).asInstanceOf[Int] > 0) score += 5 * 0.3 //访问
      if (t(5).asInstanceOf[Int] > 0) score += 5 * 0.6 //购买
      if (t(6).asInstanceOf[Byte] > 0) score += 5 * 0.1 //搜藏
      if (t(7).asInstanceOf[Double] > 0)
        score = t(7).asInstanceOf[Double] //评论

      (t(2).asInstanceOf[Int] % 10, Rating(t(0).asInstanceOf[Int], t(1).asInstanceOf[Int], score))
    }

    val numRatings = ratings.count()
    val numUsers = ratings.map(_._2.user).distinct().count()
    val numGoods = ratings.map(_._2.product).distinct().count()

    println(s"Got $numRatings ratings from $numUsers users on $numGoods movies," +())

    // split ratings into train (60%), validation (20%), and test (20%) based on the
    // last digit of the timestamp, add myRatings to train, and cache them

    val numPartitions = 4
    val training = ratings.filter(x => x._1 < 6).values.repartition(numPartitions).cache()
    val validation = ratings.filter(x => x._1 >= 6 && x._1 < 8).values.repartition(numPartitions).cache()
    val test = ratings.filter(x => x._1 >= 8).values.cache()

    val numTraining = training.count()
    val numValidation = validation.count()
    val numTest = test.count()

    println(s"Training: $numTraining, validation: $numValidation, test: $numTest")

    // train models and evaluate them on the validation set
    val ranks = List(8, 12, 20)
    val lambdas = List(0.01, 0.1, 1.0)
    val numIterations = List(10, 20, 40)
    var bestModel: Option[MatrixFactorizationModel] = None
    var bestValidationRmse = Double.MaxValue
    var bestRank = 0
    var bestLambda = -1.0
    var bestNumIter = -1
    for (rank <- ranks; lambda <- lambdas; numIter <- numIterations) {
      val model = ALS.train(training, rank, numIter, lambda)
      val validationRmse = computeRmse(model, validation)
      println(s"RMSE (validation) = $validationRmse for the model trained with rank = $rank, lambda = $lambda, and numIter = $numIter.")

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

    println(s"The best model was trained with rank = $bestRank and lambda = $bestLambda , and numIter = $bestNumIter, and its RMSE on the test set is $testRmse.")

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
    }.join(predictions)

    math.sqrt(ratesAndPreds.map { case ((user, product), (r1, r2)) =>
      val err = (r1 - r2)
      err * err
    }.mean())

  }

}
