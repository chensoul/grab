package com.javachen.grab.examples.sparksql

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object ScalaSparkSQL {
  // Define the schema using a case class.
  // Note: Case classes in Scala 2.10 can support only up to 22 fields. To work around this limit,
  // you can use custom classes that implement the Product interface.
  case class People(name: String, age: Int)

  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("ScalaSparkSQL"))
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    // this is used to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._

    // Create an RDD of People objects and register it as a table.
    val people = sc.textFile("people.txt").map(_.split(",")).map(p => People(p(0), p(1).trim.toInt)).toDF()
    people.registerTempTable("people")

    // SQL statements can be run by using the sql methods provided by sqlContext.
    val teenagers = sqlContext.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")

    // The results of SQL queries are DataFrames and support all the normal RDD operations.
    // The columns of a row in the result can be accessed by ordinal.
    teenagers.map(t => "Name: " + t(0)).collect().foreach(println)

    people.saveAsParquetFile("people.parquet")

    val parquetFile = sqlContext.parquetFile("people.parquet")
  }
}