package com.javachen.grab.mahout.cf

import com.javachen.grab.redis.RedisClient
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.{Jedis, JedisPool}

import scala.collection.mutable.ArrayBuffer
import scala.sys.process._

/**
 * 使用mahout基于物品的无偏好协同过滤
 *
 * 在mahout程序运行完成后，从mahout的输出目录读取推荐结果，对推荐结果补足数据、过滤去重，并将最后结果存入hdfs和redis
 */
object ItemBaseBoolCFAfter {
  val recNum = 40

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("ItemBaseBoolCFAfter")
    conf.registerKryoClasses(Array(classOf[Jedis], classOf[JedisPool])).set("spark.kryoserializer.buffer.mb", "16")

    val sc = new SparkContext(conf)

    val filePath = "user_goods_bool_rec"
    val hc = new HiveContext(sc)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    //获取mahout的推荐结果
    val recommends = sc.textFile(filePath+"/out").map { line =>
      val fileds = line.toString().split("\t")
      val user = fileds(0)
      val recArray = fileds(1).replace("[", "").replace("]", "").split(",").map(_.split(":")(0)).map(_.toInt)
      (user.toInt, recArray)
    }

    //获取访问记录中用户和城市记录
    val userCitys = hc.sql("select distinct user_id,city_id from dw_rec.user_goods_preference").map { t =>
      (t(0).asInstanceOf[Int], t(1).asInstanceOf[Int])
    }.repartition(16)

    //在线商品
    val onlineGoodsSp = hc.sql("select goods_id,sp_id,city_id from dw_rec.online_goods").map { t =>
      (t(0).asInstanceOf[Int], (t(1).asInstanceOf[Int], t(2).asInstanceOf[Int]))
    }.collectAsMap()
    val onlineGoodsSpBD: Broadcast[collection.Map[Int,(Int,Int)]] = sc.broadcast(onlineGoodsSp)

    //热销本地商品，按城市分组统计
    val hotCityGoods = hc.sql("select city_id,goods_id,total_amount from dw_rec.hot_goods").map { t =>
      (t(0).asInstanceOf[Int], (t(1).asInstanceOf[Int], t(2).asInstanceOf[Int]))
    }.groupByKey().map { case (city, list) =>
      (city, list.toArray.sortBy { case (goods_id, total_num) => -total_num }.take(recNum).map { case (goods_id, total_num) => goods_id })
    }
    //配送商品 city_id=9999
    val sendGoodsRdd=hc.sql("select goods_id from dw_rec.hot_goods where city_id=9999 order by total_amount desc limit 100").map { t =>
      t(0).asInstanceOf[Int]
    }
    val sendGoods = sc.broadcast(sendGoodsRdd.collect())

    //对于没有推荐到的用户，取user_id=0的推荐结果
    val otherUserRecommendsWithCity = hotCityGoods.map { case (city, array) =>
      (city, (0, Array[Int]()))
    }

    // for debug
//    val recArray= recommends.lookup(73699292).head
//    val hotArray=hotCityGoods.lookup(836).head
//    val s=  hotCityGoods.lookup(9999).head
//    val localGoods = recArray ++ hotArray ++ s
//    val city=836
//    val ss=filterGoods(localGoods,sendGoods.value,city, onlineGoodsSpBD.value).mkString(",")

    val finalReults = recommends.join(userCitys).map { case (user, (recArray, city)) =>
      (city, (user, recArray))
    }.union(otherUserRecommendsWithCity).join(hotCityGoods).map { case (city, ((user, recArray), hotArray)) =>
      val localGoods = recArray ++ hotArray
      (user, city, filterGoods(localGoods,sendGoods.value,city,onlineGoodsSpBD.value).mkString(","))
    }

    var start = System.currentTimeMillis()
    println("Begin to save result to hdfs")

    "hadoop fs -rm -r /logroot/user_goods_bool_rec".!
    finalReults.map { case (user, city, products) =>
      user + "\t" + city + "\t" + products
    }.saveAsTextFile("/logroot/"+filePath)
    println("Save to hdfs Time = " + (System.currentTimeMillis() - start) * 1.0 / 1000)

    start = System.currentTimeMillis()
    println("Begin to save result to redis")

    finalReults.mapPartitions { iter =>
      val jedis = RedisClient.pool.getResource
      while (iter.hasNext) {
        val (user, city, products) = iter.next()
        jedis.hset("g7_recommend_" + user, city.toString, products)
      }
      iter
    }.collect()
    println("Save to redis Time = " + (System.currentTimeMillis() - start) * 1.0 / 1000)

    sc.stop()
  }

  /**
   * 过滤商品，取在线商品并且一个商家只推一个商品，并且按城市进行过滤
   */
  /**
   *
   * @param localGoods 本地商品：到实体店消费的商品
   * @param sendGoods 配送商品：通过快递配送的商品
   * @param city_id 用户当前所在城市
   * @param onlineGoodsSp 所有在线商品
   * @return
   */
  def filterGoods(localGoods: Array[Int],sendGoods: Array[Int],city_id:Int, onlineGoodsSp: collection.Map[Int, (Int,Int)]) = {
    val result = ArrayBuffer[Int]()
    var filtered = collection.Map[Int, Int]()

    //处理本地商品：必须在线，一个商家推荐一个商品，商家城市和用户城市一致
    for (product <- localGoods if (result.size < recNum)) {
      onlineGoodsSp.get(product) match{
        case Some(spCity) => {
          val sp_id=spCity._1
          val sp_city_id=spCity._2
          if(!filtered.contains(sp_id) && city_id == sp_city_id){
            //sp_id -> goods_id
            filtered += sp_id -> product
            result += product
          }
        }
        case None => {}
      }
    }

    //处理配送商品：
    for (product <- sendGoods if (result.size < recNum)) {
      onlineGoodsSp.get(product) match{
        case Some(spCity) => {
          val sp_id=spCity._1
          if(!filtered.contains(sp_id)){
            //sp_id -> goods_id
            filtered += sp_id -> product
            result += product
          }
        }
        case None => {}
      }
    }
    result
  }
}
