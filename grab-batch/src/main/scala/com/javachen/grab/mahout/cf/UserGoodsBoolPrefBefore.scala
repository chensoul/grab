package com.javachen.grab.mahout.cf

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.sys.process._

object UserGoodsBoolPrefBefore {

  def main(args: Array[String]): Unit = {
    val filePath = "user_goods_recommend"
    val conf = new SparkConf().setAppName("UserGoodsBoolPrefBefore")
    val sc = new SparkContext(conf)

    val hc = new HiveContext(sc)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val ratings = hc.sql("select user_id,city_id,goods_id from dw_rec.user_goods_preference").map { t =>
      (t(0).asInstanceOf[Int], t(1).asInstanceOf[Int], t(2).asInstanceOf[Int])
    }.cache()

    "hadoop fs -rm -r user_goods_recommend/in".!

    ratings.map { case (user, city, product) =>
      (user + "," + product)
    }.saveAsTextFile(filePath+"/in")
  }
}
