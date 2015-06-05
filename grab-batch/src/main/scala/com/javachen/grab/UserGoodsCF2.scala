package com.javachen.grab

import java.util

import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.sys.process._

/**
 * 基于物品的协同过滤，用户和商品做笛卡尔连接，占用太多内存，程序无法运行
 */
object UserGoodsCF2{

  def main(args: Array[String]): Unit = {
    // 1. 环境设置
    val conf = new SparkConf().setAppName("UserGoodsCF")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //conf.set("spark.kryoserializer.buffer.mb", "64")
    conf.set("spark.kryo.classesToRegister", "scala.collection.mutable.BitSet,scala.Tuple2,scala.Tuple1,org.apache.spark.mllib.recommendation.Rating")
    conf.set("spark.akka.frameSize", "50");//单位是MB
    val sc = new SparkContext(conf)

    val hc = new HiveContext(sc)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.OFF)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

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

    // 2.给所有用户推荐在线的商品
    // 4.批量推荐

    //对每个用户取最近的一个城市
    val userCitys = hc.sql("select distinct user_id,city_id from dw_rec.user_goods_preference").map { t =>
      (t(0).asInstanceOf[Int], t(1).asInstanceOf[Int])
    }.reduceByKey((x,y)=>y).collectAsMap()

    //在线商品
    val onlineGoodsSp = hc.sql("select goods_id,sp_id from dw_rec.online_goods").map { t =>
      (t(0).asInstanceOf[Int], t(1).asInstanceOf[Int])
    }.collectAsMap()

    //热销商品，按城市分组统计
    val hotCityGoods = hc.sql("select city_id,goods_id,total_amount from dw_rec.hot_goods").map { t =>
      (t(0).asInstanceOf[Int], (t(1).asInstanceOf[Int], t(2).asInstanceOf[Int]))
    }.groupByKey().map { case (city, list) =>
      (city, list.toArray.sortBy { case (goods_id, total_num) => -total_num }.take(30).map { case (goods_id, total_num) => goods_id })
    }.collectAsMap()
    val seedGoods=hotCityGoods(9999) //配送商品 city_id=9999

    val otherUserCity=hotCityGoods.map({case (city,array)=>(0,city)}) //未推荐用户和城市映射
    val allUserCity= userCitys ++ otherUserCity

    val recommendsTopN = allUserCity.map { case (user,city) =>
      val recommendedProducts=model.recommendProducts(user, 40).map(_.product)
      val hotGoods =hotCityGoods.getOrElse(city,Array[Int]())
      val toFilterGoods=recommendedProducts ++ hotGoods ++ seedGoods
      (user,city,filterGoods(toFilterGoods,onlineGoodsSp).mkString(","))
    }

    sc.stop()

    print("All Done")
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
}
