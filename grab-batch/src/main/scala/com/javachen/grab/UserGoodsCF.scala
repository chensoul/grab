package com.javachen.grab

import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd._
import org.apache.spark.{SparkConf, SparkContext}

import sys.process._

//import org.apache.log4j.{Logger,Level}

/**
 *
 * @author <a href="mailto:junechen@163.com">june</a>.
 * @date 2015-05-19 10:18.
 */
object UserGoodsCF {
  def main(args: Array[String]): Unit = {
    //Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    //Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    // set up environment
    val sc = new SparkContext(new SparkConf().setAppName("UserGoodsCF"))
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    sqlContext.parquetFile("/logroot/user_goods_preference").registerTempTable("user_goods_preference")
    //user_id: int, goods_id: int, visit_count: int, visit_time: int, visit_city_id: int, buy_count: int, buy_time: int, buy_city_id: int, avg_score: float, collect_count: int
    val user_goods_preference = sqlContext.sql("SELECT * FROM user_goods_preference where user_id>0 limit 1000")
    val user_citys=user_goods_preference.map(t=> (t(0).asInstanceOf[Int],t(4).asInstanceOf[Int])).distinct()

    //0,7,235,1431943216,0,null,null,null,null,null
    val ratings = user_goods_preference.map { t =>
      var score = 0.0
      if (t(2).asInstanceOf[Int] > 0)
        score += 0.7 //访问

      if (t(5).asInstanceOf[Int] > 0)
        score += 0.2 //购买

      if (t(8).asInstanceOf[Float] > 0)
        score += t(8).asInstanceOf[Float] * 0.9 //评分

      if (t(9).asInstanceOf[Int] > 0)
        score += 0.1 //搜藏

      (t(3).asInstanceOf[Int] % 10, Rating(t(0).asInstanceOf[Int], t(1).asInstanceOf[Int], score))
    }

    //ratings.collect().foreach(println)

    println("Got " + ratings.count() + " ratings from "
      +  ratings.map(_._2.user).distinct().count() + " users on " + ratings.map(_._2.product).distinct().count() + "goods.")

    val rank = 12
    val numIterations = 20
    val lambda = 0.01
    val numPartitions = 4

    val training = ratings.values.repartition(numPartitions).cache()
    val model = ALS.train(training, rank, numIterations, lambda)

    val usersProducts = training.map { case Rating(user, product, rate) =>
      (user, product)
    }

    val predictions = model.predict(usersProducts).map { case Rating(user, product, rate) =>
      ((user, product), rate)
    }

    val ratesAndPreds = training.map { case Rating(user, product, rate) =>
      ((user, product), rate)
    }.join(predictions).sortByKey()

    val RMSE = math.sqrt(ratesAndPreds.map { case ((user, product), (r1, r2)) =>
      val err = (r1 - r2)
      err * err
    }.mean())

    println(s"The model was trained with rank = $rank and lambda = $lambda, and numIter = $numIterations, and its RMSE is $RMSE.")


    //确保只生成一个文件，并按用户排序
    val formatedRatesAndPreds = ratesAndPreds.repartition(1).sortBy(_._1).map({
      case ((user, product), (rate, pred)) => user + "\t" + product + "\t" + rate + "\t" + pred
    })

    "hadoop fs -rm -r /tmp/user_goods_rates".!
    formatedRatesAndPreds.saveAsTextFile("/tmp/user_goods_rates")

    //排序取10条，限制结果集为5，待确认代码是否正确
    predictions.map( { case  ((user, product), rate)=> Rating(user, product, rate)}).sortBy(- _.rating).groupBy(_.user).map(x => x._2.take(2)).take(5)

//    val result=user_citys.collect().map(t=> {
//      try {
//        val rs = model.recommendProducts(t._1, 10)
//        var value = ""
//
//        rs.foreach(r => {
//          //value = value + r.product + ":" + r.rating + ","
//          value += r.product + ","
//        })
//        Array(t._1, t._2, value.substring(0, value.length - 2)).mkString("\t")
//      }catch{
//          case ex: java.util.NoSuchElementException =>{}
//      }
//    })
//
//    val resultRDD = sc.parallelize(result,1)//并行为1，hdfs上只生成一个文件
//    resultRDD.saveAsTextFile("/tmp/user_goods_result")

     }

}
