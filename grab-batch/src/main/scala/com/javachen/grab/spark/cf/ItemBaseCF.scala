package com.javachen.grab.spark.cf

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.jblas.DoubleMatrix
import scopt.OptionParser

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.sys.process._


/**
 * 基于物品的协同过滤，矩阵相乘，内存占用太多
 */
object ItemBaseCF {

  def main(args: Array[String]): Unit = {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("ItemBaseCF") {
      head("UserGoodsCF: an example app for ALS on Goods data.")
      opt[Int]("rank")
        .text(s"rank, default: ${defaultParams.rank}}")
        .action((x, c) => c.copy(rank = x))
      opt[Int]("numIterations")
        .text(s"number of iterations, default: ${defaultParams.numIterations}")
        .action((x, c) => c.copy(numIterations = x))
      opt[Double]("lambda")
        .text(s"lambda (smoothing constant), default: ${defaultParams.lambda}")
        .action((x, c) => c.copy(lambda = x))
      opt[Unit]("kryo")
        .text("use Kryo serialization")
        .action((_, c) => c.copy(kryo = true))
      opt[Int]("trainBlocks")
        .text(s"number of train blocks, default: ${defaultParams.trainBlocks} (auto)")
        .action((x, c) => c.copy(trainBlocks = x))
      opt[Int]("topk")
        .text(s"number of recommendation result, default: ${defaultParams.topk}")
        .action((x, c) => c.copy(topk = x))
      note(
        """
          |For example:
          |
          | spark-submit --class com.javachen.grab.UserGoodsCF \
          |  --master yarn-client \
          |  --executor-memory 90g --driver-memory 4g --executor-cores 31 \
          |  grab-batch-1.0-SNAPSHOT.jar \
          |  --rank 12 --numIterations 20 --lambda 0.01 \
          |  --trainBlocks 128 --topk 10
        """.stripMargin)
    }

    parser.parse(args, defaultParams).map { params =>
      run(params)
    } getOrElse {
      System.exit(1)
    }
  }

  def run(params: Params) {
    // 1. 环境设置
    //var params=Params(12,20,0.01,true,128)

    val conf = new SparkConf().setAppName(s"UserGoodsCF with $params")
    if (params.kryo) {
      conf.registerKryoClasses(Array(classOf[mutable.BitSet], classOf[Rating])).set("spark.kryoserializer.buffer.mb", "16")
    }
    val sc = new SparkContext(conf)


    val hc = new HiveContext(sc)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    // 2. 加载数据
    val ratings = hc.sql("select user_id,goods_id,visit_count,buy_count,is_collect,avg_score from dw_rec.user_goods_preference where visit_count >1").map { t =>
      var score = 0.0
      if (t(5).asInstanceOf[Double] > 0) {
        score = t(5).asInstanceOf[Double] //评论
      } else {
        if (t(2).asInstanceOf[Int] > 0) score += 5 * 0.3 //访问
        if (t(3).asInstanceOf[Int] > 0) score += 5 * 0.6 //购买
        if (t(4).asInstanceOf[Byte] > 0) score += 5 * 0.1 //搜藏
      }
      Rating(t(0).asInstanceOf[Int], t(1).asInstanceOf[Int], score)
    }

    var start = System.currentTimeMillis()

    val trainingRatings = ratings.groupBy(_.user).filter(r => 10 <= r._2.size).flatMap(_._2).setName("trainingRatings").persist(StorageLevel.MEMORY_AND_DISK_SER)
    println(s"Got ${trainingRatings.count()} trainingRatings out of ${ratings.count()} ratings with count of goods per user > 10")

    val trainingUsers = trainingRatings.map(_.user)
    val trainingGoods = trainingRatings.map(_.product)
    println(s"TrainingRatings ratings with ${trainingUsers.distinct.count} users on ${trainingGoods.distinct.count} goods.")
    println("Count Time = " + (System.currentTimeMillis() - start) * 1.0 / 1000)

    // 3.训练模型
    start = System.currentTimeMillis()
    //val (rank,numIterations,lambda) =(12,20,0.01,128)
    val model = ALS.train(trainingRatings, params.rank, params.numIterations, params.lambda, params.trainBlocks)
    println("Train Time = " + (System.currentTimeMillis() - start) * 1.0 / 1000)

    trainingRatings.unpersist()

    //    // 4.批量推荐
    //    val userCitys = hc.sql("select distinct user_id,city_id from dw_rec.user_goods_preference").map { t =>
    //      (t(0).asInstanceOf[Int], t(1).asInstanceOf[Int])
    //    }.reduceByKey((x,y)=>y) //对每个用户取最近的一个城市
    //
    //    //热销商品，按城市分组统计
    //    val hotCityGoods = hc.sql("select city_id,goods_id,total_amount from dw_rec.hot_goods").map { t =>
    //      (t(0).asInstanceOf[Int], (t(1).asInstanceOf[Int], t(2).asInstanceOf[Int]))
    //    }.groupByKey().map { case (city, list) =>
    //      (city, list.toArray.sortBy { case (goods_id, total_num) => -total_num }.take(30).map { case (goods_id, total_num) => goods_id })
    //    }.collectAsMap()
    //
    //    //在线商品
    //    val onlineGoodsSp = hc.sql("select goods_id,sp_id from dw_rec.online_goods").map { t =>
    //      (t(0).asInstanceOf[Int], t(1).asInstanceOf[Int])
    //    }.collectAsMap()
    //    val onlineGoodsSpBD=sc.broadcast(onlineGoodsSp)

    val productFeatures = model.productFeatures.collect()
    var productArray = ArrayBuffer[Int]()
    var productFeaturesArray = ArrayBuffer[Array[Double]]()
    for ((product, features) <- productFeatures) {
      productArray += product
      productFeaturesArray += features
    }

    val productArrayBroadcast = sc.broadcast(productArray)
    val productFeatureMatrixBroadcast = sc.broadcast(new DoubleMatrix(productFeaturesArray.toArray).transpose())

    start = System.currentTimeMillis()
    val allRecs = model.userFeatures.mapPartitions { iter =>
      // Build user feature matrix for jblas
      var userFeaturesArray = ArrayBuffer[Array[Double]]()
      var userArray = new ArrayBuffer[Int]()
      while (iter.hasNext) {
        val (user, features) = iter.next()
        userArray += user
        userFeaturesArray += features
      }

      var userFeatureMatrix = new DoubleMatrix(userFeaturesArray.toArray)
      var userRecommendationMatrix = userFeatureMatrix.mmul(productFeatureMatrixBroadcast.value)
      var productArray = productArrayBroadcast.value
      var mappedUserRecommendationArray = new ArrayBuffer[String](params.topk)

      // Extract ratings from the matrix
      for (i <- 0 until userArray.length) {
        var ratingSet = mutable.TreeSet.empty(Ordering.fromLessThan[(Int, Double)](_._2 > _._2))
        for (j <- 0 until productArray.length) {
          var rating = (productArray(j), userRecommendationMatrix.get(i, j))
          ratingSet += rating
        }
        mappedUserRecommendationArray += userArray(i) + "," + ratingSet.take(params.topk).mkString(",")
      }
      mappedUserRecommendationArray.iterator
    }

    "hadoop fs -rm -r /tmp/allRecs2".!
    allRecs.saveAsTextFile("/tmp/allRecs2")
    println("Recommend Time = " + (System.currentTimeMillis() - start) * 1.0 / 1000)

    //接下来再通过rdd关联城市、热销商品？

    //    val userCityRec = allRecs.join(userCitys).map({case (user,(array,city))=>
    //      (city,user,array)
    //    })

    //    val allRecs = model.userFeatures.mapPartitions{ iter:Iterator[(Int,Array[Double])] =>
    //      val im = imBroadcast.value
    //      val idxItems =idxItemsBroadcast.value
    //      val userCitys = userCitysBD.value
    //      val hotCityGoods = hotCityGoodsBD.value
    //      val sendGoods = hotCityGoods(9999) //配送商品 city_id=9999
    //      val onlineGoodsSp = onlineGoodsSpBD.value
    //
    //      println("Begin to handle with " + iter.size +" users")
    //      iter.map{case (user, array)=>
    //        val scores = im.mmul(new DoubleMatrix(array))
    //        val sortedWithId = scores.data.zipWithIndex.sortBy(-_._1).take(40)
    //        //通过评分索引去找到对应的商品
    //        val recommendedProducts = sortedWithId.map{case (score,idx)=>idxItems.get(idx).get}
    //
    //        //补齐数据：推荐数据40+热销数据30+配送数据30，去重（一个商家只推一个商品）之后再取前40
    //        val city=userCitys.get(user).get
    //        val hotGoods =hotCityGoods.getOrElse(city,Array[Int]())
    //        val toFilterGoods=recommendedProducts ++ hotGoods ++ sendGoods
    //
    //        (user,city,filterGoods(toFilterGoods,onlineGoodsSp).mkString(","))
    //      }
    //    }


    // 5. 对于没推荐的用户给推荐热销数据加上配送数据
    //    start = System.currentTimeMillis()
    //    hotCityGoods.map { case (city,list) =>
    //      val toFilterGoods=list ++ seedGoodsBD.value
    //      (0, city, filterGoods(toFilterGoods, onlineGoodsSp))
    //    }
    //    println("Recommend Time = " + (System.currentTimeMillis() - start) * 1.0 / 1000)

    sc.stop()

    println("All Done")

  }

  /**
   * 过滤商品，取在线商品并且一个商家只推一个商品
   */
  def filterGoods(toFilterGoods: Array[Int], onlineGoodsSp: scala.collection.Map[Int, Int]) = {
    var filtered = scala.collection.mutable.Map[Int, Int]()
    var sp_id = -1

    for (product <- toFilterGoods if (filtered.size < 40)) {
      sp_id = onlineGoodsSp.get(product).getOrElse(-1)
      if (sp_id > 0 && !filtered.contains(sp_id)) {
        //sp_id -> goods_id
        filtered += sp_id -> product
      }
    }
    filtered.values
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

  case class Params(rank: Int = 12, numIterations: Int = 20, lambda: Double = 0.01, kryo: Boolean = false, trainBlocks: Int = -1, topk: Int = 10)

}
