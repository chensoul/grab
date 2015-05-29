package com.javachen.grab

import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD

/**
 *
 * Created by <a href="mailto:junechen@163.com">june</a> on 2015-05-27 09:13.
 */
object EvaluateResult {
  def coverage(training: RDD[Rating],userRecommends:RDD[(Int, List[Int])])={
    userRecommends.flatMap(_._2).distinct().count.toDouble / training.map(_.product).distinct().count
  }

  def popularity(training: RDD[Rating],userRecommends:RDD[(Int, List[Int])])={
    var ret = 0.0
    var n=0
    val item_popularity=training.map{ case Rating(user, product, rate) =>
      (product,(user, rate))
    }.groupByKey(4).map{case (product,list)=>
      (product,list.size)
    }.collectAsMap()

    userRecommends.flatMap(_._2).collect().foreach { p =>
      ret = ret + math.log(1 + item_popularity.get(p).get)
      n = n + 1
    }

    ret/n
  }

  def recallAndPrecisionAndF1(training: RDD[Rating],userRecommends:RDD[(Int, List[Int])]):(Double, Double,Double) = {
    val usersProducts: RDD[(Int, Int)] = training.map { case Rating(user, product, rate) =>
      (user, product)
    }

    val groupData=userRecommends.join(usersProducts.groupByKey().map {case (k,v) => (k,v.toList)})

    val (hit, testNum, recNum) = groupData.map{ case (user, (mItems, tItems)) =>
      var count = 0
      // 计算准确率：推荐命中商品数/实际推荐商品数, topN为推荐上限值
      val precNum = mItems.length
      for (i <- 0 until precNum)
        if (tItems.contains(mItems(i)))
          count += 1
      (count, tItems.length, precNum) }.reduce( (t1, t2) => (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3) )

      val recall: Double = hit * 1.0 / testNum
      val precision: Double = hit * 1.0 / recNum
      val f1: Double = 2 * recall * precision / (recall + precision)

      println(s"$hit,$testNum,$recNum")
      (recall,precision,f1)
  }

  def recallAndPrecision(test:RDD[Rating],result:RDD[Rating]):Double = {
    val numHit: Long = result.intersection(test).count
    val recall: Double = numHit * 1.0 / test.count
    val precision: Double = numHit * 1.0 / result.count
    val f1: Double = 2 * recall * precision / (recall + precision)
    System.out.println("recall : " + recall + "\nprecision : " + precision + "\nf1 : " + f1)
    f1
  }
}
