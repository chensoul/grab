package com.javachen.grab

import java.util

import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.sys.process._

/**
 * 基于物品的协同过滤
 */
object UserGoodsCF2{

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
    val numIterations = 20
    val lambda = 0.01
    val numPartitions = 16
    val fetchNum=50

    //表结构：user_id,goods_id,visit_time,city_id,visit_count,buy_count,is_collect,avg_score
    val data = hc.sql("select * from dw_rec.user_goods_preference_filter >=3").map { t =>
      var score = 0.0
      if (t(7).asInstanceOf[Double] > 0){
        score = t(7).asInstanceOf[Double] //评论
      }else{
        if (t(4).asInstanceOf[Int] > 0) score += 5 * 0.3 //访问
        if (t(5).asInstanceOf[Int] > 0) score += 5 * 0.6 //购买
        if (t(6).asInstanceOf[Byte] > 0) score += 5 * 0.1 //搜藏
      }
      ((t(0).asInstanceOf[Int], t(3).asInstanceOf[Int]),Rating(t(0).asInstanceOf[Int], t(1).asInstanceOf[Int], score))
    }

    val ratings = data.values.cache()
    val userCitys = data.keys.groupByKey(numPartitions).map {case (user,list)=>
      (user,list.toList.last) //对每个用户取最近的一个城市
    }

    val userNum = ratings.map(_.user).distinct().count
    val goodNum = ratings.map(_.product).distinct().count
    println("Got " + ratings.count() + " ratings from " + userNum + " users on " + goodNum + " goods. " )

    //1.训练模型
    var start = System.currentTimeMillis()
    val training =ratings.repartition(numPartitions)
    val model = ALS.train(training, rank, numIterations, lambda)
    print("Train Time = " + (System.currentTimeMillis() - start) * 1.0 / 1000)

    // 2.给所有用户推荐在线的商品
    val onlineGoodsRDD= hc.sql("select goods_id,sp_id from dw_rec.online_goods").map {t =>(t(0).asInstanceOf[Int],t(1).asInstanceOf[Int])}
    val onlineGoods = sc.broadcast(onlineGoodsRDD.collectAsMap())

    val candidates = userCitys.map(_._1).cartesian(onlineGoodsRDD.map(_._1))
    var recommendsTopN: RDD[(Int, List[Int])] = model.predict(candidates).map { case Rating(user, product, rate) =>
      (user, (product, rate))
    }.groupByKey(numPartitions).map{case (user,list)=>
      (user,list.toList.sortBy {case (product,rate)=> - rate}.take(fetchNum).map{case (product,rate)=>product})
    }

    //3.对推荐结果补齐数据、过滤、去重
    val recommendsTopNWithCity=recommendsTopN.join(userCitys, numPartitions)
    handlResult(sc,hc,recommendsTopNWithCity,onlineGoods,fetchNum,numPartitions)

    //4.保存推荐结果到redis
    ratings.unpersist()

    sc.stop()

    print("All Done")
  }

  def handlResult(sc:SparkContext,hc:HiveContext,recommendsTopNWithCity: RDD[(Int, (List[Int],Int))],onlineGoods:Broadcast[collection.Map[Int, Int]],fetchNum:Int=50,numPartitions:Int=4)={
    val hot_goods_sql="select city_id,goods_id,total_amount from dw_rec.hot_goods"
    val send_goods_sql="select goods_id from dw_rec.hot_goods where city_id=9999 order by total_amount desc limit 40"

    val hotGoodsRDD = hc.sql(hot_goods_sql).map { t =>
      (t(0).asInstanceOf[Int],(t(1).asInstanceOf[Int],t(2).asInstanceOf[Int]))
    }.groupByKey(numPartitions).map { case (user,list) =>
      val hostGoods = list.toList.sortBy{ case (product, total_num) => -total_num }.take(fetchNum).map { case (product, total_num) => product }
      (user,hostGoods )
    }
    val sendGoods = sc.broadcast(hc.sql(send_goods_sql).map {t =>(t(0).asInstanceOf[Int])}.take(fetchNum)).value

    //合并结果
    val lastUserRecommends=recommendsTopNWithCity.map{case (user,(list1,city_id))=>
      (city_id,(user,list1))
    }.join(hotGoodsRDD,numPartitions).map{case (city_id,((user,list1),list2))=>
      //推荐结果 + 热销商品 + 配送商品
      val res= {list1 ++ list2 ++ sendGoods}
      (user,city_id,filterGoods(res,onlineGoods.value).take(fetchNum)) //只取在线商品并且一个商家只推一个商品
    }

    //对其他没有被推荐的用户构造推荐列表：热销商品+配送商品
    val otherUsersRecommends=hotGoodsRDD.map{ case (city_id, list2) =>
      //热销商品+配送商品
      val res= { list2 ++ sendGoods }
      (0,city_id,filterGoods(res,onlineGoods.value).take(fetchNum))
    }

    //保存结果
    var start = System.currentTimeMillis()
    "hadoop fs -rm -r /tmp/user_goods_rates".!
    lastUserRecommends.union(otherUsersRecommends).saveAsTextFile("/tmp/user_goods_rates")
    print("SaveAsTextFile Time = " + (System.currentTimeMillis() - start) * 1.0 / 1000)

    //保存结果到redis
    start = System.currentTimeMillis()
    lastUserRecommends.union(otherUsersRecommends).foreachPartition(partitionOfRecords => {
      var values=new util.HashMap[String,String]()
      partitionOfRecords.foreach(pair => {
        val jedis = RedisClient.pool.getResource
        values = new util.HashMap[String,String]()
        values.put("city",pair._2.toString)
        values.put("rec",pair._3.mkString(","))
        jedis.hmset(pair._1.toString,values)
        jedis.close()
      })
    })
    print("SaveToRedis Time = " + (System.currentTimeMillis() - start) * 1.0 / 1000)
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
    res.foreach{ product =>
      val sp_id =onlineGoods.get(product).get
      if (!filtered.contains(sp_id)){
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
