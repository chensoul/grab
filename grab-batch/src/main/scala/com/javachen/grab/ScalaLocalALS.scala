package com.javachen.grab

import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.sys.process._

/**
 * 本地模式运行
 */
object ScalaLocalALS {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("Scala Collaborative Filtering Example"))

    // 加载并解析数据
    val data = sc.textFile("data/ml-1m/ratings.dat")

    val ratings = data.map(_.split("::") match { case Array(user, item, rate, ts) =>
      Rating(user.toInt, item.toInt, rate.toDouble)
    }).cache()

    val users = ratings.map(_.user).distinct()
    val products = ratings.map(_.product).distinct()
    println("Got "+ratings.count()+" ratings from "+users.count+" users on "+products.count+" products.")
    //Got 1000209 ratings from 6040 users on 3706 products.

    val numPartitions=4
    val training= ratings.repartition(numPartitions)

    val rank = 12
    val lambda = 0.01
    val numIterations = 20
    val model = ALS.train(training, rank, numIterations, lambda)

    //从 ratings 中获得只包含用户和商品的数据集
    val usersProducts = training.map { case Rating(user, product, rate) =>
      (user, product)
    }

    usersProducts.count  //Long = 1000209

    //使用推荐模型对用户商品进行预测评分，得到预测评分的数据集
    var predictions = model.predict(usersProducts).map { case Rating(user, product, rate) =>
        ((user, product), rate)
    }

    predictions.count //Long = 1000209

    //将真实评分数据集与预测评分数据集进行合并
    val ratesAndPreds = training.map { case Rating(user, product, rate) =>
      ((user, product), rate)
    }.join(predictions)

    ratesAndPreds.count  //Long = 1000209

    //然后计算根均方差
    val rmse= math.sqrt(ratesAndPreds.map { case ((user, product), (r1, r2)) =>
      val err = (r1 - r2)
      err * err
    }.mean())

    println(s"RMSE = $rmse")

    //保存预测评分，确保只生成一个文件，并排序
    "rm -r /tmp/result".!
    ratesAndPreds.sortByKey().repartition(1).sortBy(_._1).map({
      case ((user, product), (rate, pred)) => (user + "," + product + "," + rate + "," + pred)
    }).saveAsTextFile("/tmp/result")

    var predictionsByUser = predictions.map { case ((user, product), rate) =>
      (user, (product, rate))
    }.groupByKey(numPartitions).map{case (user_id,list)=>
      (user_id,list.toList.sortBy {case (goods_id,rate)=> - rate})
    }

    users.take(5) //Array[Int] = Array(384, 1084, 4904, 3702, 5618)
    predictionsByUser.collect.toMap.get(384).get.size //Int = 22
    predictionsByUser.collect.toMap.get(384).get.take(10)
    //List((3671,5.028573740954833),
    // (593,4.752628162378783),
    // (1304,4.320285784582784),
    // (2947,4.283574992381999),
    // (1201,4.169434788726177),
    // (3037,4.105002333711454),
    // (1207,4.00591692913912),
    // (260,4.0001475531558714),
    // (2951,3.9523150000184506),
    // (1197,3.8872705071947635))

    val myProducts= ratings.filter(_.user == 384)
    myProducts.count //Int=22
    var candidates = products.map(product => (384, product))
    candidates.count //Long = 3706
    var recommends = model.predict(candidates).sortBy(-_.rating)
    recommends.count //Long = 3706
    recommends.take(10)
    //Array(
    // Rating(384,2545,8.354966018818265),
    // Rating(384,129,8.113083736094676),
    // Rating(384,184,8.038113395650853),
    // Rating(384,811,7.983433591425284),
    // Rating(384,1421,7.912044967873945),
    // Rating(384,1313,7.719639594879865),
    // Rating(384,2892,7.53667094600392),
    // Rating(384,2483,7.295378004543803),
    // Rating(384,397,7.141158013610967),
    // Rating(384,97,7.071089782695754)
    // )
    recommends.take(10).map(_.product)
    //Array[Int] = Array(2545, 129, 184, 811, 1421, 1313, 2892, 2483, 397, 97)
    var hit=myProducts.map(_.product).collect().intersect(recommends.take(10).map(_.product)) //0
    var percent = hit.size.toDouble / myProducts.count //0

    candidates= products.subtract(myProducts.map(_.product)).map(x => (384, x))
    candidates.count //Long = 3684
    recommends = model.predict(candidates).sortBy(-_.rating)
    recommends.count //Long = 3684
    recommends.take(10).map(_.product)
    //Array[Int] = Array(2545, 129, 184, 811, 1421, 1313, 2892, 2483, 397, 97)

    hit=myProducts.map(_.product).collect().intersect(recommends.take(10).map(_.product)) //0
    percent = hit.size.toDouble / myProducts.count //0

    var recommendsArray=model.recommendProducts(384, 10)
    //Array(
    // Rating(384,2545,8.354966018818265),
    // Rating(384,129,8.113083736094676),
    // Rating(384,184,8.038113395650853),
    // Rating(384,811,7.983433591425284),
    // Rating(384,1421,7.912044967873945),
    // Rating(384,1313,7.719639594879865),
    // Rating(384,2892,7.53667094600392),
    // Rating(384,2483,7.295378004543803),
    // Rating(384,397,7.141158013610967),
    // Rating(384,97,7.071089782695754))


    var res=users.take(1).flatMap { user =>
      model.recommendProducts(user, 10)
    }

    res.map(_.product)

    var start = System.currentTimeMillis()
    candidates = users.cartesian(products)
    var recommendsByUserTopN = model.predict(candidates).map { case Rating(user, product, rate) =>
      (user, (product, rate))
    }.groupByKey(numPartitions).map{case (user,list)=>
      (user,list.toList.sortBy {case (product,rate)=> - rate}.take(10).map{case (product,rate)=>product})
    }

    "rm -rf data/recommendsByUserTopN".!
    recommendsByUserTopN.saveAsTextFile("data/recommendsByUserTopN")
    println("Cost Time = " + (System.currentTimeMillis() - start) * 1.0 / 1000)

    recommendsByUserTopN.collect.toMap.get(384).get
    //List((2545,8.354966018818265),
    // (129,8.113083736094676),
    // (184,8.038113395650853),
    // (811,7.983433591425284),
    // (1421,7.912044967873945),
    // (1313,7.719639594879865),
    // (2892,7.53667094600392),
    // (2483,7.295378004543803),
    // (397,7.141158013610967),
    // (97,7.071089782695754))


    EvaluateResult.coverage(training,recommendsByUserTopN)
    EvaluateResult.popularity(training,recommendsByUserTopN)
    EvaluateResult.recallAndPrecisionAndF1(training,recommendsByUserTopN)

  }
}
