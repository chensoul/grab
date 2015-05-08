package com.javachen.grab.examples.sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.List;

public class JavaSparkSQLBySchema {
    public static void main(String[] args) throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName("JavaSparkSQLBySchema");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);

        // Load a text file and convert each line to a JavaBean.
        JavaRDD<String> people = sc.textFile("people.txt");

        // The schema is encoded in a string
        String schemaString = "name age";

        // Generate the schema based on the string of schema
        List<StructField> fields = new ArrayList<StructField>();
        for (String fieldName : schemaString.split(" ")) {
            fields.add(DataType.createStructField(fieldName, DataType.StringType, true));
        }
        StructType schema = DataType.createStructType(fields);

        // Convert records of the RDD (people) to Rows.
        JavaRDD<Row> rowRDD = people.map(
                new Function<String, Row>() {
                    public Row call(String record) throws Exception {
                        String[] fields = record.split(",");
                        return Row.create(fields[0], fields[1].trim());
                    }
                });

        // Apply the schema to the RDD.
        DataFrame peopleDataFrame = sqlContext.createDataFrame(rowRDD, schema);

        // Register the DataFrame as a table.
        peopleDataFrame.registerTempTable("people");

        // SQL can be run over RDDs that have been registered as tables.
        DataFrame results = sqlContext.sql("SELECT name FROM people");

        // The results of SQL queries are DataFrames and support all the normal RDD operations.
        // The columns of a row in the result can be accessed by ordinal.
        List<String> names = results.map(new Function<Row, String>() {
            public String call(Row row) {
                return "Name: " + row.getString(0);
            }
        }).collect();
    }

}