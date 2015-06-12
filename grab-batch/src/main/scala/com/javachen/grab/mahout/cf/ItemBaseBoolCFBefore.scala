package com.javachen.grab.mahout.cf

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.sys.process._

/**
 * 使用mahout基于物品的无偏好协同过滤
 *
 * 在mahout程序运行前，从hive中查询数据取出用户和商品映射关系，存入mahout推荐程序的输入目录
 */
object ItemBaseBoolCFBefore {

  def main(args: Array[String]): Unit = {
    val filePath = "user_goods_bool_rec"
    val conf = new SparkConf().setAppName("ItemBaseBoolCFBefore")
    val sc = new SparkContext(conf)

    val hc = new HiveContext(sc)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val ratings = hc.sql("select user_id,goods_id from dw_rec.user_goods_preference").map { t =>
      (t(0).asInstanceOf[Int], t(1).asInstanceOf[Int])
    }

    "hadoop fs -rm -r user_goods_bool_rec/in".!

    ratings.map { case (user, product) =>
      (user + "," + product)
    }.saveAsTextFile(filePath+"/in")

    sc.stop()
  }
}
