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
 * 基于物品的协同过滤给用户进行推荐
 */
object UserGoodsCF {
  def main(args: Array[String]): Unit = {


    // set up environment
    val conf = new SparkConf().setAppName("UserGoodsCF")
    conf.registerKryoClasses(Array(classOf[mutable.BitSet], classOf[Rating])).set("spark.kryoserializer.buffer.mb", "100")
    conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.akka.frameSize", "500m");

    val sc = new SparkContext(conf)
    val hc = new HiveContext(sc)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val userGoodsRDD = hc.sql("SELECT * FROM dw_recommender.user_goods_preference where user_id>0")
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
    val usersProducts = ratings.map { case Rating(user, product, rate) =>
      (user, product)
    }

    //每个用户取前40个预测结果
    val predictions = model.predict(usersProducts).map { case Rating(user, product, rate) =>
        (user, (product, rate))}.groupByKey(4).map(t=>(t._1,t._2.toList.sortBy(x=> - x._2).take(40).map{case (goods_id,rate)=>goods_id}))

    val hot_goods_sql="select city_id,goods_id,total_amount from dw_recommender.hot_goods"
    val send_goods_sql="select goods_id from dw_recommender.hot_goods where city_id=9999 order by total_amount desc"
    val goods_sp_sql="select goods_id,sp_id from dw_recommender.online_goods"
    val goodsSpRDD = hc.sql(goods_sp_sql).map({t =>(t(0).asInstanceOf[Int],t(1).asInstanceOf[Int])})
    val userCityRDD = userGoodsRDD.map(t => (t(0).asInstanceOf[Int], t(4).asInstanceOf[Int]))
    //每个城市热销商品取20个
    val hotGoodsRDD = hc.sql(hot_goods_sql).map(t => (t(0).asInstanceOf[Int],(t(1).asInstanceOf[Int],t(2).asInstanceOf[Int]))).groupByKey(4).map(t=>
      (t._1,t._2.toList.sortBy(x=> - x._2).take(20).map{case (goods_id,total_num)=>goods_id} ))
    //配送商品取10个
    val sendGoods = sc.broadcast(hc.sql(send_goods_sql).map(t =>(t(0).asInstanceOf[Int])).take(10)).value


    //3.合并结果：推荐40个+热销商品20个+配送商品10个
    start = System.currentTimeMillis()
    var userRecommendations=predictions.join(userCityRDD).map({case (user_id,(list1,city_id))=>
      (city_id,(user_id,list1))
    }).join(hotGoodsRDD).map({case (city_id,((user_id,list1),list2))=>
      var list=list1
      list ++= list2
      list ++= sendGoods

      (user_id,city_id,list)
    })

    //4.去重

    println("Not exist User Recommendation Time = " + (System.currentTimeMillis() - start) * 1.0 / 1000)

    "hadoop fs -rm -r /tmp/user_goods_rates".!
    System.setProperty("spark.akka.frameSize","1024m")
//    sc.parallelize(sc.broadcast(userRecommendations).value,1).saveAsTextFile("/tmp/user_goods_rates")

    //5.保存推荐结果到redis

    training.unpersist()
    userGoodsRDD.unpersist()

    sc.stop()
  }

  /**
   * 去重，一个商家只推一个商品，并取40个结果
   * @param sc
   * @param recommendations
   * @param goodsSpRDD
   * @return
   */
  def removeDupplicate(sc:SparkContext,recommendations:List[Int],goodsSpRDD:RDD[(Int, Int)]): String ={
    recommendations.mkString(",")
//    sc.parallelize(recommendations).map(goods_id=>(goods_id,1)).join(goodsSpRDD).map({
//      case (goods_id,(1, sp_id)) => (sp_id, goods_id)
//    }).groupBy(_._1).map({t=>t._2.take(1)}).map(_.head._2).collect().take(40).mkString(",")
  }

}
