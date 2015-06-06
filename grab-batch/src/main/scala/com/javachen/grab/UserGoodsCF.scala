package com.javachen.grab

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.jblas.DoubleMatrix

import scala.collection.mutable.ArrayBuffer
import scala.sys.process._
import org.apache.spark.SparkContext._


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
    conf.set("spark.akka.frameSize", "200");//单位是MB
    val sc = new SparkContext(conf)

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val hc = new HiveContext(sc)

    // 2. 加载数据
    val ratings = hc.sql("select user_id,goods_id,visit_count,buy_count,is_collect,avg_score from dw_rec.user_goods_preference where visit_count >5").map { t =>
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
    val model = ALS.train(training, rank, numIterations, lambda, 128)
    println("Train Time = " + (System.currentTimeMillis() - start) * 1.0 / 1000)

    training.unpersist()

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

    val productArray = model.productFeatures.keys.collect()
    val productArrayBroadcast = sc.broadcast(productArray)

    val productFeaturesArray = model.productFeatures.values.collect()
    val productFeatureMatrixBroadcast = sc.broadcast(new DoubleMatrix(productFeaturesArray).transpose())


    val allRecs = model.userFeatures.mapPartitions { iter =>
      // Build user feature matrix for jblas
      val userFeaturesArray= ArrayBuffer[Array[Double]]()
      val userArray = ArrayBuffer[Int]()
      while (iter.hasNext) {
        val (user, features) = iter.next()
        userArray += user
        userFeaturesArray += features
      }
      val userFeatureMatrix = new DoubleMatrix(userFeaturesArray.toArray)

      // Multiply the user feature and product feature matrices using jblas
      val userRecommendationArray = userFeatureMatrix.mmul(productFeatureMatrixBroadcast.value).toArray2
      val mappedUserRecommendationArray = ArrayBuffer[(Int, ArrayBuffer[Int])]()

      val productArrayLength=productArrayBroadcast.value.length
      // Extract ratings from the matrix
      for (i <- 0 until userArray.length) {
        var ratingArray = ArrayBuffer[(Int,Double)]()
        for (j <- 0 until productArrayLength) {
          val rating = (productArrayBroadcast.value(j), userRecommendationArray(i)(j))
          ratingArray += rating
        }
        // Take the top 10 recommendations for the user
        val recom =(userArray(i), ratingArray.sortBy(-_._2).take(10).map(_._1))
        mappedUserRecommendationArray += recom
      }
      mappedUserRecommendationArray.iterator
    }

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

    "hadoop fs -rm -r /tmp/allRecs2".!

    start = System.currentTimeMillis()
    allRecs.saveAsTextFile("/tmp/allRecs2")
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
