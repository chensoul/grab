package com.javachen.grab

import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.Option
import scala.collection.mutable
import scala.sys.process._
import org.apache.log4j.{Logger,Level}

/**
 * 基于物品的协同过滤
 */
object UserGoodsCF {
  def main(args: Array[String]): Unit = {
    // set up environment
    val conf = new SparkConf().setAppName("UserGoodsCF")
    conf.registerKryoClasses(Array(classOf[mutable.BitSet], classOf[Rating])).set("spark.kryoserializer.buffer.mb", "100")
    conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.akka.frameSize", "500");

    val sc = new SparkContext(conf)
    val hc = new HiveContext(sc)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //表结构：user_id, goods_id, visit_count, visit_time, visit_city_id, buy_count, buy_time, buy_city_id, avg_score, collect_count
    val userGoodsRDD = hc.sql("select * from dw_recommender.user_goods_preference where user_id>0")
    val ratings = userGoodsRDD.map { t =>
      var score = 0.0
      if (t(2).asInstanceOf[Int] > 0) score += 5 * 0.7 //访问
      if (t(5).asInstanceOf[Int] > 0) score += 5 * 0.2 //购买
      if (t(9).asInstanceOf[Int] > 0) score += 5 * 0.1 //搜藏
      if (t(8).asInstanceOf[Double] > 0)
        score = t(8).asInstanceOf[Double] * 0.9 //评论
      Rating(t(0).asInstanceOf[Int], t(1).asInstanceOf[Int], score)
    }.cache()

    val numRatings = ratings.count()
    val numUsers = ratings.map(_.user).distinct().count()
    val numGoods = ratings.map(_.product).distinct().count()
    println(s"Got $numRatings ratings from $numUsers users on $numGoods goods.")

    //1.训练模型
    val rank = 12
    val numIterations = 20
    val lambda = 0.01
    val numPartitions = 4
    var start = System.currentTimeMillis()
    val training = ratings.repartition(numPartitions)
    val model = ALS.train(training, rank, numIterations, lambda)
    println("Train Time = " + (System.currentTimeMillis() - start) * 1.0 / 1000)

    // 2.预测评分
    start = System.currentTimeMillis()
    val usersProducts = ratings.map { case Rating(user, product, rate) =>
      (user, product)
    }

    val fetchNum=50
    //对每个用户按评分取前50个预测结果
    val predictions = model.predict(usersProducts).map { case Rating(user, product, rate) =>
      (user, (product, rate))}.groupByKey(numPartitions).map(t=>(t._1,t._2.toList.sortBy(x=> - x._2).take(fetchNum).map{case (goods_id,rate)=>goods_id}))

    val hot_goods_sql="select city_id,goods_id,total_amount from dw_recommender.hot_goods"
    val send_goods_sql="select goods_id from dw_recommender.hot_goods where city_id=9999 order by total_amount desc"
    val online_goods_sql="select distinct goods_id,sp_id from dw_recommender.online_goods"

    val onlineGoodsRDD= hc.sql(online_goods_sql).map {t =>(t(0).asInstanceOf[Int],t(1).asInstanceOf[Int])}
    //对每个用户取最近购买的一个城市
    val userCityRDD = userGoodsRDD.map(t => (t(0).asInstanceOf[Int], t(4).asInstanceOf[Int])).groupByKey(numPartitions).map{case (user_id,list)=>
      (user_id,list.toList.last)
    }

    //每个城市热销商品取50个
    val hotGoodsRDD = hc.sql(hot_goods_sql).map(t =>
      (t(0).asInstanceOf[Int],(t(1).asInstanceOf[Int],t(2).asInstanceOf[Int]))).groupByKey(numPartitions).map(t=>
      (t._1,t._2.toList.sortBy(x=> - x._2).take(fetchNum).map{case (goods_id,total_num)=>goods_id} ))
    //配送商品取50个
    val sendGoods = sc.broadcast(hc.sql(send_goods_sql).map(t =>(t(0).asInstanceOf[Int])).take(fetchNum)).value
    val onlineGoods= sc.broadcast(onlineGoodsRDD.collectAsMap()).value

    //3.合并结果：推荐+热销商品+配送商品，只取在线商品并且一个商家只推一个商品
    val userRecommendations=predictions.join(userCityRDD,1).map({case (user_id,(list1,city_id))=>
      (city_id,(user_id,list1))
    }).join(hotGoodsRDD,4).map({case (city_id,((user_id,list1),list2))=>
      //推荐结果 + 热销商品 + 配送商品
      val res= {list1 ++ list2 ++ sendGoods}
      (user_id,city_id,filterGoods(res,onlineGoods))
    })

    //对于不在推荐用户列表中的用户，补上推荐结果，统一设置user_id=0
    userRecommendations.union(hotGoodsRDD.map{ case (city_id, list2) =>
      //热销商品+配送商品
      val res= { list2 ++ sendGoods }
      (0,city_id,filterGoods(res,onlineGoods))
    })

    //保存结果
    "hadoop fs -rm -r /tmp/user_goods_rates".!
    userRecommendations.saveAsTextFile("/tmp/user_goods_rates")
    println("User Recommendation Time = " + (System.currentTimeMillis() - start) * 1.0 / 1000)

    //5.保存推荐结果到redis
    ratings.unpersist()

    sc.stop()

    println("All Done")
  }

  /**
   * 过滤商品，取在线商品并且一个商家只推一个商品
   *
   * @param res
   * @param onlineGoods
   * @return 返回前40条结果
   */
  def filterGoods(res:List[Int],onlineGoods:scala.collection.Map[Int,Int]): Iterable[Int] ={
    var filtered=scala.collection.mutable.Map[Int,Int]()
    res.foreach{ goods_id =>
      if (onlineGoods.contains(goods_id) && !filtered.contains(onlineGoods.get(goods_id).get)){
        //sp_id -> goods_id
        filtered += onlineGoods.get(goods_id).get -> goods_id
      }
    }
    filtered.values.take(40)
  }

}
