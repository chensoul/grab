package com.javachen.grab

import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, ALS, Rating}
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

    val ratings = data.map { line =>
      val fields = line.split("::")
      Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble)
    }.cache()

    val users = ratings.map(_.user).distinct()
    val goods = ratings.map(_.product).distinct()
    println("Got "+ratings.count()+" ratings from "+users.count+" users on "+goods.count+" goods.")
    //Got 1000209 ratings from 6040 users on 3706 goods.

    val rank = 12
    val lambda = 0.01
    val numIterations = 20
    val numPartitions=4

    val training: RDD[Rating] = ratings.repartition(numPartitions).cache()
    val model = ALS.train(training, rank, numIterations, lambda)

    //从 ratings 中获得只包含用户和商品的数据集
    val usersProducts: RDD[(Int, Int)] = training.map { case Rating(user, product, rate) =>
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

    //然后计算均方差
    val rmse= math.sqrt(ratesAndPreds.map { case ((user, product), (r1, r2)) =>
      val err = (r1 - r2)
      err * err
    }.mean())

    println(s"RMSE = $rmse")

    //保存预测评分，确保只生成一个文件，并排序
    "rm -r /tmp/user_goods_rates".!
    ratesAndPreds.sortByKey().repartition(1).sortBy(_._1).map({
      case ((user, product), (rate, pred)) => (user + "," + product + "," + rate + "," + pred)
    }).saveAsTextFile("/tmp/user_goods_rates")

    val resultGroupByUser = predictions.map { case ((user, product), rate) =>
      (user, (product, rate))
    }.groupByKey(numPartitions).map{case (user_id,list)=>
      (user_id,list.toList.sortBy {case (goods_id,rate)=> - rate}.map{case (goods_id,rate)=>goods_id})
    }

    resultGroupByUser.count //Long = 6040
    resultGroupByUser.collect.toMap.get(1).get.size //53
    //data/ml-1m/ratings.dat文件中用户id为1的记录有53条

    users.take(5) //Array[Int] = Array(384, 1084, 4904, 3702, 5618)

    resultGroupByUser.collect.toMap.get(384).get.size //Int = 22
    val myProducts= ratings.filter(_.user == 384)
    myProducts.count //Int=22
    var candidates = goods.map(goods_id => (384, goods_id))
    candidates.count //Long = 3706
    var recommends = model.predict(candidates).sortBy(-_.rating)
    recommends.count //Long = 3706
    recommends.take(10)
    var hit=myProducts.map(_.product).collect().intersect(recommends.take(10).map(_.product)) //0
    var percent = hit.size.toDouble / myProducts.count //0

    resultGroupByUser.collect.toMap.get(384).get
    //List[Int] = List(3671, 593, 1304, 2947, 1207, 1201, 2951, 1197, 1610, 3074, 260, 3037, 204, 3508, 599, 3494, 3487, 673, 1007, 2055, 1381, 2896)
    recommends.take(10).map(_.product)
    //Array[Int] = Array(2342, 1793, 3161, 2658, 649, 807, 3847, 201, 2435, 2913)

    candidates= goods.subtract(myProducts.map(_.product)).map(x => (384, x)) //Long = 3684
    recommends.count //Long = 3684
    recommends.take(10).map(_.product)
    //Array[Int] = Array(2342, 1793, 3161, 2658, 649, 807, 3847, 201, 2435, 2913)
    hit=myProducts.map(_.product).collect().intersect(recommends.take(10).map(_.product)) //0
    percent = hit.size.toDouble / myProducts.count //0


    users.take(5).map { user_id =>
      val myProducts= ratings.filter(_.user == user_id).map(_.product).collect
      // val candidates = goods.subtract(myProducts).map(x => (user_id, x))

      //预测所有商品
      val candidates = goods.map(goods_id => (user_id, goods_id))
      //      val recommends = model.predict(candidates).top(10)(Ordering[Double].on(_.product))
      val recommends = model.predict(candidates).sortBy(-_.rating).take(100).map(_.product)

      val percent = myProducts.intersect(recommends).size.toDouble / myProducts.size

      user_id+","+recommends.size+","+myProducts.size+","+percent
    }

    val result=model.recommendProducts(384, 1000).toList.map(_.product)
    //Array[Int] = Array(2342, 1793, 3161, 2658, 649, 807, 3847, 201, 2435, 2913)
    hit=myProducts.map(_.product).collect().intersect(result) //0
    percent = hit.size.toDouble / myProducts.count //0

    var res=users.take(1).flatMap { user_id =>
      model.recommendProducts(user_id, 10)
    }

    res.map(_.product)

    candidates = users.cartesian(goods)
    predictions = model.predict(candidates).map { case Rating(user, product, rate) =>
      ((user, product), rate)
    }

    //每个用户获取50个推荐结果
    val userRecommends: RDD[(Int, List[Int])] = predictions.map { case ((user, product), rate) =>
      (user, (product, rate))
    }.groupByKey(numPartitions).map{case (user_id,list)=>
      (user_id,list.toList.sortBy {case (goods_id,rate)=> - rate}.take(10).map{case (goods_id,rate)=>goods_id})
    }

    userRecommends.collect.toMap.get(384).get
    //List(2545, 129, 184, 811, 1421, 1313, 2892, 2483, 397, 97)

    EvaluateResult.coverage(training,userRecommends)
    EvaluateResult.popularity(training,userRecommends)
    EvaluateResult.recallAndPrecisionAndF1(training,userRecommends)

  }
}
