package com.javachen.grab.examples.sparksql

import org.apache.spark.{SparkConf, SparkContext}

object ScalaSparkSQLBySchema {

  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("ScalaSparkSQL"))
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    // Create an RDD
    val people = sc.textFile("people.txt")

    // The schema is encoded in a string
    val schemaString = "name age"

    // Import Spark SQL data types and Row.
    import org.apache.spark.sql._

    // Generate the schema based on the string of schema
    val schema =
      StructType(
        schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

    // Convert records of the RDD (people) to Rows.
    val rowRDD = people.map(_.split(",")).map(p => Row(p(0), p(1).trim))

    // Apply the schema to the RDD.
    val peopleDataFrame = sqlContext.createDataFrame(rowRDD, schema)

    // Register the DataFrames as a table.
    peopleDataFrame.registerTempTable("people")

    // SQL statements can be run by using the sql methods provided by sqlContext.
    val results = sqlContext.sql("SELECT name FROM people")

    // The results of SQL queries are DataFrames and support all the normal RDD operations.
    // The columns of a row in the result can be accessed by ordinal.
    results.map(t => "Name: " + t(0)).collect().foreach(println)
  }
}