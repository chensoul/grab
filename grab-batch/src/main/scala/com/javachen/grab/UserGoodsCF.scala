package com.javachen.grab

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import scala.sys.process._


/**
 * 基于物品的协同过滤
 */
object UserGoodsCF {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("UserGoodsCF")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryoserializer.buffer.mb", "64")
    conf.set("spark.kryo.classesToRegister", "scala.collection.mutable.BitSet,scala.Tuple2,scala.Tuple1,org.apache.spark.mllib.recommendation.Rating")
    conf.set("spark.akka.frameSize", "500");
    val sc = new SparkContext(conf)

    val hc = new HiveContext(sc)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //表结构：user_id,goods_id,visit_time,city_id,visit_count,buy_count,is_collect,avg_score
    val ratings = hc.sql("select * from dw_rec.user_goods_preference_filter").map { t =>
      var score = 0.0
      if (t(7).asInstanceOf[Double] > 0) {
        score = t(7).asInstanceOf[Double] //评论
      } else {
        if (t(4).asInstanceOf[Int] > 0) score += 5 * 0.3 //访问
        if (t(5).asInstanceOf[Int] > 0) score += 5 * 0.6 //购买
        if (t(6).asInstanceOf[Byte] > 0) score += 5 * 0.1 //搜藏
      }
      //((user,city),Rating)
      ((t(0).asInstanceOf[Int], t(3).asInstanceOf[Int]), Rating(t(0).asInstanceOf[Int], t(1).asInstanceOf[Int], score))
    }

    val userCitys = ratings.keys.groupByKey().map { case (user, list) => (user, list.last) } //对每个用户取最近的一个城市

    "hadoop fs -rm -r /tmp/rec/user_goods_rates".!
    ratings.values.repartition(32).map{case Rating(user, product, rate) =>
      user+","+product+","+rate
    }.saveAsTextFile("/tmp/rec/user_goods_rates")


    "hadoop fs -rm -r /tmp/rec/temp /tmp/rec/recommendations /tmp/rec/out".!

    "mahout splitDataset -i /tmp/rec/user_goods_rates -o /tmp/rec/dataset --trainingPercentage 0.9 --probePercentage 0.1".!

    ("mahout parallelALS -i /tmp/rec/user_goods_rates -o /tmp/rec/out --implicitFeedback true --alpha 0.8 --numFeatures 2 --numIterations 10 --numThreadsPerSolver 2 --lambda 0.01 --tempDir /tmp/rec/temp").!
    ​
    "mahout evaluateFactorization -i /tmp/rec/dataset/probeSet/ -o /tmp/rec/out/rmse/ --userFeatures /tmp/rec/out/U/ --itemFeatures /tmp/rec/out/M/".!

    "mahout recommendfactorized -i /tmp/rec/out/userRatings/ -o /tmp/rec/recommendations/ --userFeatures /tmp/rec/out/U/ --itemFeatures /tmp/rec/out/M/ --numRecommendations 40 --numThreads 4 --maxRating 5".!

    sc.stop()

    println("All Done")
  }


}
