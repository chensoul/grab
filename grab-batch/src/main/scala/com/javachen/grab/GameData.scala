package com.javachen.grab

import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * Created by <a href="mailto:junechen@163.com">june</a> on 2015-05-26 16:21.
 */
object GameData {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("dt")
      .set("spark.executor.memory", "8g")
      .set("spark.cores.max", "10")
    val sc = new SparkContext(conf)

    val data = sc.textFile("data/game.txt")

    val parsedData = data.map { line =>
      val parts = line.split(",")
      val gameId = parts(0)
      val userId = parts(1)
      ((userId, gameId), 1)
    }
    val aggregData = parsedData.reduceByKey(_ + _)
    val ad = aggregData.map { x =>
      x match {
        case ((userId, gameId), number) => (userId, (gameId, number))
      }
    }

    val groupData = ad.groupByKey()
    val indexData = groupData.zipWithIndex()

    val flatData = indexData.map { x =>
      var str = ""
      val arr = x._1._2.toArray
      for (i <- 0 to arr.size - 2) {
        str += x._1._1 + "," + x._2 + "," + arr(i)._1 + "," + arr(i)._2 + "\n"
      }
      str += x._1._1 + "," + x._2 + "," + arr(arr.size - 1)._1 + "," + arr(arr.size - 1)._2
      str
    }


    flatData.saveAsTextFile("data/parsed_game")
    val t = flatData.take(30)
    println(t mkString "\n")
  }

}