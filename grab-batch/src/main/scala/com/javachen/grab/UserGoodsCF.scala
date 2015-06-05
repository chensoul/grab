package com.javachen.grab

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.jblas.DoubleMatrix
import scala.sys.process._


/**
 * 基于物品的协同过滤
 */
object UserGoodsCF {
  def main(args: Array[String]): Unit = {
    // 1. 环境设置
    val conf = new SparkConf().setAppName("UserGoodsCF")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //conf.set("spark.kryoserializer.buffer.mb", "64")
    conf.set("spark.kryo.classesToRegister", "scala.collection.mutable.BitSet,scala.Tuple2,scala.Tuple1,org.apache.spark.mllib.recommendation.Rating")
    conf.set("spark.akka.frameSize", "50");//单位是MB
    val sc = new SparkContext(conf)

    Logger.getLogger("org.apache.spark").setLevel(Level.INFO)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val hc = new HiveContext(sc)

    // 2. 加载数据
    val ratings = hc.sql("select user_id,goods_id,visit_count,buy_count,is_collect,avg_score from dw_rec.user_goods_preference").map { t =>
      var score = 0.0
      if (t(5).asInstanceOf[Double] > 0) {
        score = t(5).asInstanceOf[Double] //评论
      } else {
        if (t(2).asInstanceOf[Int] > 0) score += 5 * 0.3 //访问
        if (t(3).asInstanceOf[Int] > 0) score += 5 * 0.6 //购买
        if (t(4).asInstanceOf[Byte] > 0) score += 5 * 0.1 //搜藏
      }
      Rating(t(0).asInstanceOf[Int], t(1).asInstanceOf[Int], score)
    }.repartition(4)

    val training = ratings.setName("training").persist(StorageLevel.MEMORY_AND_DISK_SER)

    val trainingUsers = training.map(_.user).distinct()
    val trainingGoods = training.map(_.product).distinct()
    var start = System.currentTimeMillis()
    println("Got " + training.count() + " ratings from " + trainingUsers.count + " users on " + trainingGoods.count + " goods. ")
    println("Count Time = " + (System.currentTimeMillis() - start) * 1.0 / 1000)

    // 3.训练模型
    val (rank,numIterations,lambda) = (12,20,0.01)
    start = System.currentTimeMillis()
    val model = ALS.train(training, rank, numIterations, lambda)
    println("Train Time = " + (System.currentTimeMillis() - start) * 1.0 / 1000)

    training.unpersist()

    // 4.批量推荐

    //对每个用户取最近的一个城市
    val userCitys = hc.sql("select distinct user_id,city_id from dw_rec.user_goods_preference").map { t =>
      (t(0).asInstanceOf[Int], t(1).asInstanceOf[Int])
    }.reduceByKey((x,y)=>y).collectAsMap()
    val userCitysBD=sc.broadcast(userCitys)

    //在线商品
    val onlineGoodsSp = hc.sql("select goods_id,sp_id from dw_rec.online_goods").map { t =>
      (t(0).asInstanceOf[Int], t(1).asInstanceOf[Int])
    }.collectAsMap()
    val onlineGoodsSpBD=sc.broadcast(onlineGoodsSp)

    //热销商品，按城市分组统计
    val hotCityGoods = hc.sql("select city_id,goods_id,total_amount from dw_rec.hot_goods").map { t =>
      (t(0).asInstanceOf[Int], (t(1).asInstanceOf[Int], t(2).asInstanceOf[Int]))
    }.groupByKey().map { case (city, list) =>
      (city, list.toArray.sortBy { case (goods_id, total_num) => -total_num }.take(30).map { case (goods_id, total_num) => goods_id })
    }.collectAsMap()
    val hotCityGoodsBD=sc.broadcast(hotCityGoods)
    val seedGoodsBD=sc.broadcast(hotCityGoods(9999)) //配送商品 city_id=9999

    val itemFactors = model.productFeatures.values.collect()
    val itemMatrix = new DoubleMatrix(itemFactors)
    val imBroadcast = sc.broadcast(itemMatrix)
    val idxItems=model.productFeatures.keys.zipWithIndex().map{case (array, idx) => (idx,array)}.collectAsMap()
    val idxItemsBroadcast = sc.broadcast(idxItems)

    val allRecs = model.userFeatures.map{ case (user, array) =>
      val userVector = new DoubleMatrix(array)
      val scores = imBroadcast.value.mmul(userVector)
      val sortedWithId = scores.data.zipWithIndex.sortBy(-_._1)
      //通过评分索引去找到对应的商品
      val recommendedProducts = sortedWithId.take(40).map{case (score,idx)=>idxItemsBroadcast.value.get(idx).get}

      //补齐数据：推荐数据40+热销数据30+配送数据30，去重（一个商家只推一个商品）之后再取前40
      val city=userCitysBD.value.get(user).get
      val hotGoods =hotCityGoodsBD.value.getOrElse(city,Array[Int]())
      val toFilterGoods=recommendedProducts ++ hotGoods ++ seedGoodsBD.value
      (user,city,filterGoods(toFilterGoods,onlineGoodsSpBD.value).mkString(","))
    }

//    var res=model.userFeatures.coalesce(4).cartesian(model.productFeatures.values.coalesce(4)).map{ case ((user, userArray), itemArray) =>
//      val itemMatrix = new DoubleMatrix(itemArray)
//      val userVector = new DoubleMatrix(userArray)
//      val scores = itemMatrix.mmul(userVector)
//      val sortedWithId = scores.data.zipWithIndex.sortBy(-_._1)
//      val recommendedProducts = sortedWithId.map(_._2).take(10).map{idx=>idxProductsBroadcast.value.get(idx).get}.mkString(",")
//      (user, recommendedProducts)
//    }

//    model.recommendProducts(10,10).map(_.product)

    "hadoop fs -rm -r /tmp/allRecs".!

    start = System.currentTimeMillis()
    allRecs.saveAsTextFile("/tmp/allRecs")
    println("Recommend Time = " + (System.currentTimeMillis() - start) * 1.0 / 1000)

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
}
