package com.javachen.grab

import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.jblas.DoubleMatrix
import scala.sys.process._

import org.apache.log4j.{Level, Logger}


/**
 * 本地模式运行
 */
object ScalaLocalALS {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("Scala Collaborative Filtering Example"))

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    // 1. 加载并解析数据
    val data = sc.textFile("data/ml-1m/ratings.dat")

    val ratings = data.map(_.split("::") match { case Array(user, item, rate, ts) =>
      Rating(user.toInt, item.toInt, rate.toDouble)
    }).cache()

    val users = ratings.map(_.user).distinct()
    val products = ratings.map(_.product).distinct()
    println("Got "+ratings.count()+" ratings from "+users.count+" users on "+products.count+" products.")
    //Got 1000209 ratings from 6040 users on 3706 products.

    // 2. 训练模型
    val rank = 12
    val lambda = 0.01
    val numIterations = 20
    val model = ALS.train(ratings, rank, numIterations, lambda)

    model.userFeatures
    model.userFeatures.count

    model.productFeatures
    model.productFeatures.count

    // 3. 计算均方差
    //从 ratings 中获得只包含用户和商品的数据集
    val usersProducts = ratings.map { case Rating(user, product, rate) =>
      (user, product)
    }

    usersProducts.count  //Long = 1000209

    //使用推荐模型对用户商品进行预测评分，得到预测评分的数据集
    var predictions = model.predict(usersProducts).map { case Rating(user, product, rate) =>
        ((user, product), rate)
    }

    predictions.count //Long = 1000209

    //将真实评分数据集与预测评分数据集进行合并
    val ratesAndPreds = ratings.map { case Rating(user, product, rate) =>
      ((user, product), rate)
    }.join(predictions)

    ratesAndPreds.count  //Long = 1000209

    val rmse= math.sqrt(ratesAndPreds.map { case ((user, product), (r1, r2)) =>
      val err = (r1 - r2)
      err * err
    }.mean())

    println(s"RMSE = $rmse")

    // 4.保存预测评分，确保只生成一个文件，并排序
    "rm -r /tmp/result".!
    ratesAndPreds.sortByKey().repartition(1).sortBy(_._1).map({
      case ((user, product), (rate, pred)) => (user + "," + product + "," + rate + "," + pred)
    }).saveAsTextFile("/tmp/result")

    //对预测的评分结果按用户进行分组并按评分倒排序
    predictions.map { case ((user, product), rate) =>
      (user, (product, rate))
    }.groupByKey().map{case (user_id,list)=>
      (user_id,list.toList.sortBy {case (goods_id,rate)=> - rate})
    }

    // 5. 对某一个用户推荐所有商品
    users.take(5) //Array[Int] = Array(384, 1084, 4904, 3702, 5618)
    val userId = users.take(1)(0) //384
    val K = 10
    val topKRecs = model.recommendProducts(userId, K)
    //topKRecs: Array[org.apache.spark.mllib.recommendation.Rating] = Array(Rating(384,1539,7.360670267591244), Rating(384,219,6.736019537477872), Rating(384,1520,6.730562698267339), Rating(384,775,6.697620546404394), Rating(384,3161,6.49555676613329), Rating(384,2711,6.445916831219404), Rating(384,2503,6.428273027496898), Rating(384,771,6.4255234943275825), Rating(384,853,6.170422982870869), Rating(384,759,6.04929517890501))

    println(topKRecs.mkString("\n"))
//    Rating(384,1539,7.360670267591244)
//    Rating(384,219,6.736019537477872)
//    Rating(384,1520,6.730562698267339)
//    Rating(384,775,6.697620546404394)
//    Rating(384,3161,6.49555676613329)
//    Rating(384,2711,6.445916831219404)
//    Rating(384,2503,6.428273027496898)
//    Rating(384,771,6.4255234943275825)
//    Rating(384,853,6.170422982870869)
//    Rating(384,759,6.04929517890501)

    val productsForUser=ratings.keyBy(_.user).lookup(384)
//    Seq[org.apache.spark.mllib.recommendation.Rating] = WrappedArray(Rating(384,2055,2.0), Rating(384,1197,4.0), Rating(384,593,5.0), Rating(384,599,3.0), Rating(384,673,2.0), Rating(384,3037,4.0), Rating(384,1381,2.0), Rating(384,1610,4.0), Rating(384,3074,4.0), Rating(384,204,4.0), Rating(384,3508,3.0), Rating(384,1007,3.0), Rating(384,260,4.0), Rating(384,3487,3.0), Rating(384,3494,3.0), Rating(384,1201,5.0), Rating(384,3671,5.0), Rating(384,1207,4.0), Rating(384,2947,4.0), Rating(384,2951,4.0), Rating(384,2896,2.0), Rating(384,1304,5.0))

    productsForUser.size //Int = 22
    productsForUser.sortBy(-_.rating).take(10).map(rating => (rating.product, rating.rating)).foreach(println)
//    (593,5.0)
//    (1201,5.0)
//    (3671,5.0)
//    (1304,5.0)
//    (1197,4.0)
//    (3037,4.0)
//    (1610,4.0)
//    (3074,4.0)
//    (204,4.0)
//    (260,4.0)

    /* Compute squared error between a predicted and actual rating */
    // We'll take the first rating for our example user 789
    val actualRating = productsForUser.take(1)(0)
    //actualRating: org.apache.spark.mllib.recommendation.Rating = Rating(384,2055,2.0)    val predictedRating = model.predict(789, actualRating.product)
    val predictedRating = model.predict(384, actualRating.product)
    //predictedRating: Double = 1.9426030777174637

    //找出和2055商品最相似的商品
    val itemId = 2055
    val itemFactor = model.productFeatures.lookup(itemId).head
    //itemFactor: Array[Double] = Array(0.3660752773284912, 0.43573060631752014, -0.3421429991722107, 0.44382765889167786, -1.4875195026397705, 0.6274569630622864, -0.3264533579349518, -0.9939845204353333, -0.8710321187973022, -0.7578890323638916, -0.14621856808662415, -0.7254264950752258)
    val itemVector = new DoubleMatrix(itemFactor)
    //itemVector: org.jblas.DoubleMatrix = [0.366075; 0.435731; -0.342143; 0.443828; -1.487520; 0.627457; -0.326453; -0.993985; -0.871032; -0.757889; -0.146219; -0.725426]

    cosineSimilarity(itemVector, itemVector)
    // res99: Double = 0.9999999999999999

    val sims = model.productFeatures.map{ case (id, factor) =>
      val factorVector = new DoubleMatrix(factor)
      val sim = cosineSimilarity(factorVector, itemVector)
      (id, sim)
    }
    val sortedSims = sims.top(K)(Ordering.by[(Int, Double), Double] { case (id, similarity) => similarity })
    //sortedSims: Array[(Int, Double)] = Array((2055,0.9999999999999999), (2051,0.9138311231145874), (3520,0.8739823400539756), (2190,0.8718466671129721), (2050,0.8612639515847019), (1011,0.8466911667526461), (2903,0.8455764332511272), (3121,0.8227325520485377), (3674,0.8075743004357392), (2016,0.8063817280259447))
    println(sortedSims.mkString("\n"))
//    (2055,0.9999999999999999)
//    (2051,0.9138311231145874)
//    (3520,0.8739823400539756)
//    (2190,0.8718466671129721)
//    (2050,0.8612639515847019)
//    (1011,0.8466911667526461)
//    (2903,0.8455764332511272)
//    (3121,0.8227325520485377)
//    (3674,0.8075743004357392)
//    (2016,0.8063817280259447)

    val sortedSims2 = sims.top(K + 1)(Ordering.by[(Int, Double), Double] { case (id, similarity) => similarity })
    //sortedSims2: Array[(Int, Double)] = Array((2055,0.9999999999999999), (2051,0.9138311231145874), (3520,0.8739823400539756), (2190,0.8718466671129721), (2050,0.8612639515847019), (1011,0.8466911667526461), (2903,0.8455764332511272), (3121,0.8227325520485377), (3674,0.8075743004357392), (2016,0.8063817280259447), (3672,0.8016276723120674))

    sortedSims2.slice(1, 11).map{ case (id, sim) => (id, sim) }.mkString("\n")
//    (2051,0.9138311231145874)
//    (3520,0.8739823400539756)
//    (2190,0.8718466671129721)
//    (2050,0.8612639515847019)
//    (1011,0.8466911667526461)
//    (2903,0.8455764332511272)
//    (3121,0.8227325520485377)
//    (3674,0.8075743004357392)
//    (2016,0.8063817280259447)
//    (3672,0.8016276723120674)

    //计算给该用户推荐的前K个商品的平均准确度MAPK
    val actualProducts= productsForUser.map(_.product)
    //actualProducts: Seq[Int] = ArrayBuffer(2055, 1197, 593, 599, 673, 3037, 1381, 1610, 3074, 204, 3508, 1007, 260, 3487, 3494, 1201, 3671, 1207, 2947, 2951, 2896, 1304)
    val predictedProducts= topKRecs.map(_.product)
    //predictedProducts:Array[Int] = Array(1539, 219, 1520, 775, 3161, 2711, 2503, 771, 853, 759)
    val apk10 = avgPrecisionK(actualProducts, predictedProducts, 10)
    // apk10: Double = 0.0

    users.collect.flatMap { user =>
      model.recommendProducts(user, 10)
    }

    //计算所有的推荐结果
    val itemFactors = model.productFeatures.map { case (prodcut, factor) => factor }.collect()
    val itemMatrix = new DoubleMatrix(itemFactors)
    println(itemMatrix.rows, itemMatrix.columns)

    val imBroadcast = sc.broadcast(itemMatrix)

    var idxProducts=model.productFeatures.map { case (prodcut, factor) => prodcut }.zipWithIndex().map{case (prodcut, idx) => (idx,prodcut)}.collectAsMap()
    val idxProductsBroadcast = sc.broadcast(idxProducts)

    val allRecs = model.userFeatures.map{ case (user, array) =>
      val userVector = new DoubleMatrix(array)
      val scores = imBroadcast.value.mmul(userVector)
      val sortedWithId = scores.data.zipWithIndex.sortBy(-_._1)
      val recommendedProducts = sortedWithId.map(_._2).map{idx=>idxProductsBroadcast.value.get(idx).get}
      (user, recommendedProducts)  //recommendedIds 为索引
    }

    //验证结果是否正确
    allRecs.lookup(384).head.take(10)
    //res50: Array[Int] = Array(1539, 219, 1520, 775, 3161, 2711, 2503, 771, 853, 759)
    topKRecs.map(_.product)
    //res49: Array[Int] = Array(1539, 219, 1520, 775, 3161, 2711, 2503, 771, 853, 759)


    //得到每个用户评分过的所有商品
    val userProducts = ratings.map{ case Rating(user, product, rating) => (user, product) }.groupBy(_._1)

    // finally, compute the APK for each user, and average them to find MAPK
    val MAPK = allRecs.join(userProducts).map{ case (userId, (predictedProducts, actualList)) =>
      val actualProducts = actualList.map{case (user, product)=>product}.toSeq
      avgPrecisionK(actualProducts, predictedProducts, K)
    }.reduce(_ + _) / allRecs.count
    println("Mean Average Precision at K = " + MAPK)

    // MSE, RMSE and MAE
    import org.apache.spark.mllib.evaluation.RegressionMetrics

    val predictedAndTrue = ratesAndPreds.map { case ((user, product), (actual, predicted)) => (actual, predicted) }
    val regressionMetrics = new RegressionMetrics(predictedAndTrue)
    println("Mean Squared Error = " + regressionMetrics.meanSquaredError)
    println("Root Mean Squared Error = " + regressionMetrics.rootMeanSquaredError)
    // Mean Squared Error = 0.08231947642632852
    // Root Mean Squared Error = 0.2869137090247319

    // MAPK
    import org.apache.spark.mllib.evaluation.RankingMetrics
    val predictedAndTrueForRanking = allRecs.join(userProducts).map{ case (userId, (predicted, actualWithIds)) =>
      val actual = actualWithIds.map(_._2)
      (predicted.toArray, actual.toArray)
    }
    val rankingMetrics = new RankingMetrics(predictedAndTrueForRanking)
    println("Mean Average Precision = " + rankingMetrics.meanAveragePrecision)
    // Mean Average Precision = 0.07171412913757183

    // Compare to our implementation, using K = 2000 to approximate the overall MAP
    val MAPK2000 = allRecs.join(userProducts).map{ case (userId, (predicted, actualWithIds)) =>
      val actual = actualWithIds.map(_._2).toSeq
      avgPrecisionK(actual, predicted, 2000)
    }.reduce(_ + _) / allRecs.count
    println("Mean Average Precision = " + MAPK2000)

//    recommendsByUserTopN.foreachPartition(partitionOfRecords => {
//        partitionOfRecords.foreach(pair => {
//          val jedis = RedisClient.pool.getResource
//          jedis.set(pair._1.toString,pair._2.mkString(","))
//          jedis.close()
//        })
//      })

  }

  /* Compute the cosine similarity between two vectors */
  def cosineSimilarity(vec1: DoubleMatrix, vec2: DoubleMatrix): Double = {
    vec1.dot(vec2) / (vec1.norm2() * vec2.norm2())
  }

  /* Function to compute average precision given a set of actual and predicted ratings */
  // Code for this function is based on: https://github.com/benhamner/Metrics
  def avgPrecisionK(actual: Seq[Int], predicted: Seq[Int], k: Int): Double = {
    val predK = predicted.take(k)
    var score = 0.0
    var numHits = 0.0
    for ((p, i) <- predK.zipWithIndex) {
      if (actual.contains(p)) {
        numHits += 1.0
        score += numHits / (i.toDouble + 1.0)
      }
    }
    if (actual.isEmpty) {
      1.0
    } else {
      score / scala.math.min(actual.size, k).toDouble
    }
  }
}
