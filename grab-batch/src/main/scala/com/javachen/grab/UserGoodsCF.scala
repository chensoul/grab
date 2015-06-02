package com.javachen.grab

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * 基于物品的协同过滤
 */
object UserGoodsCF {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("UserGoodsCF")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[mutable.BitSet],classOf[(Int, Int)],classOf[Rating]))
    conf.set("spark.kryoserializer.buffer.mb", "16")
    //conf.set("spark.kryo.registrator", "MyRegistrator")
    conf.set("spark.akka.frameSize", "500");

    val sc = new SparkContext(conf)
    val hc = new HiveContext(sc)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.OFF)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val rank = 12
    val numIterations = 2
    val lambda = 0.01
    val numPartitions = 16

    //表结构：user_id,goods_id,visit_time,city_id,visit_count,buy_count,is_collect,avg_score
    val data = hc.sql("select * from dw_rec.user_goods_preference_filter where goods_count >=3").map { t =>
      var score = 0.0
      if (t(7).asInstanceOf[Double] > 0) {
        score = t(7).asInstanceOf[Double] //评论
      } else {
        if (t(4).asInstanceOf[Int] > 0) score += 5 * 0.3 //访问
        if (t(5).asInstanceOf[Int] > 0) score += 5 * 0.6 //购买
        if (t(6).asInstanceOf[Byte] > 0) score += 5 * 0.1 //搜藏
      }
      ((t(0).asInstanceOf[Int], t(3).asInstanceOf[Int]), Rating(t(0).asInstanceOf[Int], t(1).asInstanceOf[Int], score))
    }

    val ratings = data.values.cache()
    val userCitys = data.keys.groupByKey(numPartitions).map { case (user, list) => (user, list.toList.last)} //对每个用户取最近的一个城市

    val userNum = ratings.map(_.user).distinct().count
    val goodNum = ratings.map(_.product).distinct().count
    println("Got " + ratings.count() + " ratings from " + userNum + " users on " + goodNum + " goods. " )

    //1.训练模型
    var start = System.currentTimeMillis()
    val training = ratings.repartition(numPartitions)
    val model = ALS.train(training, rank, numIterations, lambda)
    println("Train Time = " + (System.currentTimeMillis() - start) * 1.0 / 1000)

    //3.给所有用户推荐在线的商品
    val onlineGoodsData = hc.sql("select goods_id,sp_id from dw_rec.online_goods").map { t => (t(0).asInstanceOf[Int], t(1).asInstanceOf[Int]) }
    val onlineGoodsBC = sc.broadcast(onlineGoodsData.collectAsMap())

    val hotGoodsData = hc.sql("select city_id,goods_id,total_amount from dw_rec.hot_goods").map { t =>
      (t(0).asInstanceOf[Int], (t(1).asInstanceOf[Int], t(2).asInstanceOf[Int]))
    }
    val hotGoodsBC = sc.broadcast(hotGoodsData.groupByKey(numPartitions).map { case (city_id, list) =>
      val hostGoods = list.toList.sortBy { case (product, total_num) => -total_num }.take(30).map { case (product, total_num) => product }
      (city_id, hostGoods)
    }.collectAsMap())

    val sendGoodsBC = sc.broadcast(hc.sql("select goods_id from dw_rec.hot_goods where city_id=9999 order by total_amount desc limit 30").map { t =>
      (t(0).asInstanceOf[Int])
    }.collect())

    //关联上用户编号为0的用户，实际只给部分用户，客户端通过user_id查询推荐结果，如果查询不到，则查询user_id=0的数据
    val allUserCity = sc.broadcast(userCitys.union(hotGoodsData.map(_._1).distinct().map { case city_id => (0, city_id) }).collect()).value

    start = System.currentTimeMillis()
    var idx = 0
    allUserCity.take(100000).par.map { case (user, city) =>
      //3.对推荐结果补齐数据
      var recommendGoods =Array[Int]()
      if (user > 0){
        try{
          recommendGoods =  model.recommendProducts(user, 40).map(_.product)
        }catch{
          case ex:Exception=>None
        }
      }
      val toFilterGoods = recommendGoods ++ hotGoodsBC.value.get(city).getOrElse(List[Int]()) ++ sendGoodsBC.value

      idx += 1
      if (idx % 50 == 0) println("Finish " + idx)

      //过滤、去重
      filterGoods(toFilterGoods, onlineGoodsBC.value)
    }
    println("Recommend Time = " + (System.currentTimeMillis() - start) * 1.0 / 1000)

    ratings.unpersist()
    sc.stop()

    println("All Done")
  }

  /**
   * 过滤商品，取在线商品并且一个商家只推一个商品
   *
   * @param toFilterGoods
   * @param onlineGoods
   * @return
   */
  def filterGoods(toFilterGoods: Array[Int], onlineGoods: scala.collection.Map[Int, Int]): String = {
    var filtered = scala.collection.mutable.Map[Int, Int]()
    var sp_id = -1
    for(product<-toFilterGoods if (filtered.size<40)){
      sp_id = onlineGoods.get(product).getOrElse(-1)
      if (sp_id > 0 && !filtered.contains(sp_id)) {
        //sp_id -> goods_id
        filtered += sp_id -> product
      }
    }
    filtered.values.mkString(",")
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
