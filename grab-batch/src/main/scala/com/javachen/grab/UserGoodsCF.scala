package com.javachen.grab

import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, ALS, Rating}
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
    val conf = new SparkConf().setAppName("UserGoodsCF")
    conf.registerKryoClasses(Array(classOf[mutable.BitSet], classOf[Rating])).set("spark.kryoserializer.buffer.mb", "100")
    conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.akka.frameSize", "500");

    val sc = new SparkContext(conf)
    val hc = new HiveContext(sc)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val rank = 12
    val numIterations = 20
    val lambda = 0.01
    val numPartitions = 4
    val fetchNum=50

    //表结构：user_id,goods_id,visit_time,city_id,visit_count,buy_count,is_collect,avg_score
    val data = hc.sql("select a.* from dw_rec.user_goods_preference a join ( " +
      "select user_id,count(goods_id) cnt from dw_rec.user_goods_preference group by user_id having cnt >=10 and cnt <10000" +
      ") b on a.user_id=b.user_id").map { t =>
      var score = 0.0
      if (t(4).asInstanceOf[Int] > 0) score += 5 * 0.3 //访问
      if (t(5).asInstanceOf[Int] > 0) score += 5 * 0.6 //购买
      if (t(6).asInstanceOf[Byte] > 0) score += 5 * 0.1 //搜藏
      if (t(7).asInstanceOf[Double] > 0)
        score = t(7).asInstanceOf[Double] //评论
      ((t(0).asInstanceOf[Int], t(3).asInstanceOf[Int]),Rating(t(0).asInstanceOf[Int], t(1).asInstanceOf[Int], score))
    }

    val userCitys: RDD[(Int, Int)] = data.keys.groupByKey(numPartitions).map {case (user_id,list)=>
      (user_id,list.toList.last) //对每个用户取最近的一个城市
    }

    val ratings=data.values.cache()
    val users = ratings.map(_.user).distinct()
    val goods = ratings.map(_.product).distinct()
    println("Got "+ratings.count()+" ratings from "+users.count+" users on "+goods.count+" goods.")

    //1.训练模型
    val splits = ratings.repartition(numPartitions).randomSplit(Array(0.8, 0.2), seed = 111l)
    val training = splits(0).repartition(numPartitions)
    val test = splits(1).repartition(numPartitions)

    //训练模型
    var start = System.currentTimeMillis()
    val model = ALS.train(training, rank, numIterations, lambda)
    println("Train Time = " + (System.currentTimeMillis() - start) * 1.0 / 1000)

    //计算根均方差
    val rmse=computeRmse(model,test)
    println(s"RMSE = $rmse")

    // 2.给所有用户预测评分
    val candidates = test.map(_.user).distinct().cartesian(goods)
    var recommendsByUserTopN = model.predict(candidates).map { case Rating(user, product, rate) =>
      (user, (product, rate))
    }.groupByKey(numPartitions).map{case (user_id,list)=>
      (user_id,list.toList.sortBy {case (goods_id,rate)=> - rate}.take(fetchNum).map{case (goods_id,rate)=>goods_id})
    }

    //3.对推荐结果补齐数据、过滤、去重
    start = System.currentTimeMillis()
    handlResult(sc,hc,recommendsByUserTopN,userCitys,fetchNum,numPartitions)
    println("User Recommendation Time = " + (System.currentTimeMillis() - start) * 1.0 / 1000)


    //4.保存推荐结果到redis
    ratings.unpersist()

    sc.stop()

    println("All Done")
  }

  def handlResult(sc:SparkContext,hc:HiveContext,recommendsByUserTopN: RDD[(Int, List[Int])],userCitys: RDD[(Int, Int)],fetchNum:Int=50,numPartitions:Int=4)={
    val hot_goods_sql="select city_id,goods_id,total_amount from dw_rec.hot_goods"
    val send_goods_sql="select goods_id from dw_rec.hot_goods where city_id=9999 order by total_amount desc limit 100"
    val online_goods_sql="select distinct goods_id,sp_id from dw_rec.online_goods"

    val onlineGoodsRDD= hc.sql(online_goods_sql).map {t =>(t(0).asInstanceOf[Int],t(1).asInstanceOf[Int])}

    val hotGoodsRDD = hc.sql(hot_goods_sql).map { t =>
      (t(0).asInstanceOf[Int],(t(1).asInstanceOf[Int],t(2).asInstanceOf[Int]))
    }.groupByKey(numPartitions).map { case (user_id,list) =>
      val hostGoods = list.toList.sortBy{ case (goods_id, total_num) => -total_num }.take(fetchNum).map { case (goods_id, total_num) => goods_id }
      (user_id,hostGoods )
    }
    val sendGoods = sc.broadcast(hc.sql(send_goods_sql).map {t =>(t(0).asInstanceOf[Int])}.take(fetchNum)).value
    val onlineGoods= sc.broadcast(onlineGoodsRDD.collectAsMap()).value

    //合并结果
    val lastUserRecommends=(recommendsByUserTopN.join(userCitys, 1)).map{case (user_id,(list1,city_id))=>
      (city_id,(user_id,list1))
    }.join(hotGoodsRDD,4).map{case (city_id,((user_id,list1),list2))=>
      //推荐结果 + 热销商品 + 配送商品
      val res= {list1 ++ list2 ++ sendGoods}
      (user_id,city_id,filterGoods(res,onlineGoods)) //只取在线商品并且一个商家只推一个商品
    }

    //对其他没有被推荐的用户构造推荐列表：热销商品+配送商品
    val otherUsersRecommends=hotGoodsRDD.map{ case (city_id, list2) =>
      //热销商品+配送商品
      val res= { list2 ++ sendGoods }
      (0,city_id,filterGoods(res,onlineGoods))
    }

    //保存结果
    "hadoop fs -rm -r /tmp/user_goods_rates".!
    lastUserRecommends.union(otherUsersRecommends).saveAsTextFile("/tmp/user_goods_rates")
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
