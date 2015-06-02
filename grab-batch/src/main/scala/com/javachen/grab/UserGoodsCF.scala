package com.javachen.grab

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 基于物品的协同过滤
 */
object UserGoodsCF {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("UserGoodsCF")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryoserializer.buffer.mb", "64")
    conf.set("spark.kryo.classesToRegister", "scala.collection.mutable.BitSet,scala.Tuple2,scala.Tuple1,org.apache.spark.mllib.recommendation.Rating")
    conf.set("spark.akka.frameSize", "500");
    val sc = new SparkContext(conf)

    val hc = new HiveContext(sc)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val (training, userCitys) = preparation(hc)

    recommend(hc, training, userCitys)

    sc.stop()

    println("All Done")
  }

  def preparation(hc: HiveContext) = {
    //表结构：user_id,goods_id,visit_time,city_id,visit_count,buy_count,is_collect,avg_score
    val ratings = hc.sql("select * from dw_rec.user_goods_preference_filter").map { t =>
      var score = 0.0
      if (t(7).asInstanceOf[Double] > 0) {
        score = t(7).asInstanceOf[Double] //评论
      } else {
        if (t(4).asInstanceOf[Int] > 0) score += 5 * 0.3 //访问
        if (t(5).asInstanceOf[Int] > 0) score += 5 * 0.6 //购买
        if (t(6).asInstanceOf[Byte] > 0) score += 5 * 0.1 //搜藏
      }
      //((user,city),Rating)
      ((t(0).asInstanceOf[Int], t(3).asInstanceOf[Int]), Rating(t(0).asInstanceOf[Int], t(1).asInstanceOf[Int], score))
    }.repartition(128)

    val userCitys = ratings.keys.groupByKey().map { case (user, list) => (user, list.last) }.collect() //对每个用户取最近的一个城市
    val training = ratings.values.setName("training").persist(StorageLevel.MEMORY_AND_DISK_SER)

    (training, userCitys)
  }

  def recommend(hc: HiveContext, training: RDD[Rating], userCitys: Array[(Int, Int)]) = {
    val trainingUsers = training.map(_.user).distinct(4)
    val trainingGoods = training.map(_.product).distinct(4)
    var start = System.currentTimeMillis()
    println("Got " + training.count() + " ratings from " + trainingUsers.count + " users on " + trainingGoods.count + " goods. ")
    println("Count Time = " + (System.currentTimeMillis() - start) * 1.0 / 1000)

    val rank = 12
    val numIterations = 20
    val lambda = 0.01
    start = System.currentTimeMillis()
    val model = ALS.trainImplicit(training, rank, numIterations, lambda, 1.0)
    println("Train Time = " + (System.currentTimeMillis() - start) * 1.0 / 1000)

    val onlineGoodsSp = hc.sql("select goods_id,sp_id from dw_rec.online_goods").map { t =>
      (t(0).asInstanceOf[Int], t(1).asInstanceOf[Int])
    }.collectAsMap()
    val hotGoods = hc.sql("select city_id,goods_id,total_amount from dw_rec.hot_goods").map { t =>
      (t(0).asInstanceOf[Int], (t(1).asInstanceOf[Int], t(2).asInstanceOf[Int]))
    }.groupByKey().map { case (city_id, list) =>
      (city_id, list.toArray.sortBy { case (goods_id, total_num) => -total_num }.take(30).map { case (goods_id, total_num) => goods_id })
    }.collectAsMap()
    val sendGoods = hotGoods.get(9999).getOrElse(Array()) //配送商品，city=9999

    //关联上用户编号为0的用户：客户端通过user_id查询推荐结果，如果查询不到，则查询user_id=0的数据
    val allUserCity = userCitys ++ hotGoods.keys.map { case city_id => (0, city_id) }

    start = System.currentTimeMillis()
    var idx = 0
    val someRecommendations = allUserCity.par.take(1000).map { case (user, city) =>
      idx += 1
      if (idx % 10 == 0) println("Finish " + idx)

      val recommends = if (user > 0) model.recommendProducts(user, 40).map(_.product) else Array[Int]()
      (user, city, filterGoods(recommends, hotGoods.get(city).getOrElse(Array()), sendGoods, onlineGoodsSp))
    }

    println("Recommend Time = " + (System.currentTimeMillis() - start) * 1.0 / 1000)

    unpersist(model)
  }

  /**
   * 过滤商品，取在线商品并且一个商家只推一个商品
   */
  def filterGoods(recommends: Array[Int], hotGoods: Array[Int], sendGoods: Array[Int], onlineGoodsSp: scala.collection.Map[Int, Int]): String = {
    val toFilterGoods = recommends ++ hotGoods ++ sendGoods
    var filtered = scala.collection.mutable.Map[Int, Int]()
    var sp_id = -1

    for (product <- toFilterGoods if (filtered.size < 40)) {
      sp_id = onlineGoodsSp.get(product).getOrElse(-1)
      if (sp_id > 0 && !filtered.contains(sp_id)) {
        //sp_id -> goods_id
        filtered += sp_id -> product
      }
    }
    filtered.values.mkString(",")
  }

  def unpersist(model: MatrixFactorizationModel): Unit = {
    // At the moment, it's necessary to manually unpersist the RDDs inside the model
    // when done with it in order to make sure they are promptly uncached
    model.userFeatures.unpersist()
    model.productFeatures.unpersist()
  }

  //https://github.com/sryza/aas/blob/master/ch03-recommender/src/main/scala/com/cloudera/datascience/recommender/RunRecommender.scala
  def evaluate() = {

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
