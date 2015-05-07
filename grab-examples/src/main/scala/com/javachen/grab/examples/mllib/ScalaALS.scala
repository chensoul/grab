package com.javachen.grab.examples.mllib

import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.{SparkConf, SparkContext}

object ScalaALS {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("Scala Collaborative Filtering Example"))

    // 加载并解析数据
    val data = sc.textFile("data/mllib/als/test.data")

    val ratings = data.map(_.split(',') match { case Array(user, product, rate) =>
      Rating(user.toInt, product.toInt, rate.toDouble)
    })

    //使用ALS训练数据建立推荐模型
    val rank = 10
    val numIterations = 20
    val model = ALS.train(ratings, rank, numIterations, 0.01)

    //从 ratings 中获得只包含用户和商品的数据集
    val usersProducts = ratings.map { case Rating(user, product, rate) =>
      (user, product)
    }

    //使用推荐模型对用户商品进行预测评分，得到预测评分的数据集
    val predictions =
      model.predict(usersProducts).map { case Rating(user, product, rate) =>
        ((user, product), rate)
      }

    //将真实评分数据集与预测评分数据集进行合并
    val ratesAndPreds = ratings.map { case Rating(user, product, rate) =>
      ((user, product), rate)
    }.join(predictions)

    //然后计算均方差，注意这里没有调用 math.sqrt方法
    val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) =>
      val err = (r1 - r2)
      err * err
    }.mean()

    //打印出均方差值
    println("Mean Squared Error = " + MSE)
    //Mean Squared Error = 1.37797097094789E-5
  }

}
