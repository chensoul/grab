package com.javachen.grab.mahout.cf

import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.sys.process._

/**
 * 使用mahout基于物品的无偏好协同过滤
 *
 * 在mahout程序运行完成后，从mahout的输出目录读取推荐结果，对推荐结果补足数据、过滤去重，并将最后结果存入hdfs和redis
 */
object ItemBaseBoolCFAfter {
  val recNum = 40

  def main(args: Array[String]): Unit = {
    val filePath = "user_goods_bool_rec"
    val conf = new SparkConf().setAppName("ItemBaseBoolCFAfter")
    val sc = new SparkContext(conf)

    val hc = new HiveContext(sc)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val recommends = sc.textFile(filePath+"/out").map { line =>
      val fileds = line.toString().split("\t")
      val user = fileds(0)
      val recArray = fileds(1).replace("[", "").replace("]", "").split(",").map(_.split(":")(0)).map(_.toInt)
      (user.toInt, recArray)
    }

    val ratings = hc.sql("select user_id,city_id,goods_id from dw_rec.user_goods_preference").map { t =>
      (t(0).asInstanceOf[Int], t(1).asInstanceOf[Int], t(2).asInstanceOf[Int])
    }

    val userCitys = ratings.groupBy(_._1).map { case (user, list) =>
      (user, list.last._2)
    }

    //在线商品
    val onlineGoodsSp = hc.sql("select goods_id,sp_id from dw_rec.online_goods").map { t =>
      (t(0).asInstanceOf[Int], t(1).asInstanceOf[Int])
    }.collectAsMap()
    val onlineGoodsSpBD: Broadcast[collection.Map[Int, Int]] = sc.broadcast(onlineGoodsSp)

    //热销商品，按城市分组统计
    val hotCityGoods = hc.sql("select city_id,goods_id,total_amount from dw_rec.hot_goods").map { t =>
      (t(0).asInstanceOf[Int], (t(1).asInstanceOf[Int], t(2).asInstanceOf[Int]))
    }.groupByKey().map { case (city, list) =>
      (city, list.toArray.sortBy { case (goods_id, total_num) => -total_num }.take(30).map { case (goods_id, total_num) => goods_id })
    }
    val sendGoods = sc.broadcast(hotCityGoods.lookup(9999).head) //配送商品 city_id=9999

    //对于没有推荐到的用户，取user_id=0的推荐结果
    val otherUserRecommendsWithCity = hotCityGoods.map { case (city, array) =>
      (city, (0, Array[Int]()))
    }

    val finalReults = recommends.join(userCitys).map { case (user, (recArray, city)) =>
      (city, (user, recArray))
    }.union(otherUserRecommendsWithCity).join(hotCityGoods).map { case (city, ((user, recArray), hotArray)) =>
      val toFilterGoods = recArray ++ hotArray ++ sendGoods.value
      (user, city, filterGoods(toFilterGoods, onlineGoodsSpBD.value))
    }

    "hadoop fs -rm -r /logroot/user_goods_bool_rec".!
    finalReults.map { case (user, city, productArray) =>
      user + "\t" + city + "\t" + productArray.mkString(",")
    }.saveAsTextFile("/logroot/"+filePath)

    //    finalReults.mapPartitions { iter =>
    //      val jedis = RedisClient.pool.getResource
    //      val result = Array()
    //      while (iter.hasNext) {
    //        val (user, city, products) = iter.next()
    //        jedis.hset("g7_recommend_" + user, "city", city.toString)
    //        jedis.hset("g7_recommend_" + user, "goods_set", products.toString)
    //      }
    //      result.iterator
    //    }

    sc.stop()
  }

  /**
   * 过滤商品，取在线商品并且一个商家只推一个商品
   */
  def filterGoods(toFilterGoods: Array[Int], onlineGoodsSp: collection.Map[Int, Int]) = {
    var filtered = collection.Map[Int, Int]()
    var sp_id = -1

    for (product <- toFilterGoods if (filtered.size < recNum)) {
      sp_id = onlineGoodsSp.get(product).getOrElse(-1)
      if (sp_id > 0 && !filtered.contains(sp_id)) {
        //sp_id -> goods_id
        filtered += sp_id -> product
      }
    }
    filtered.values
  }
}
