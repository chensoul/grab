package com.javachen.grab.examples.sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * @author <a href="mailto:june.chan@foxmail.com">june</a>.
 * @date 2015-03-27 16:50.
 */
public class JavaSparkSQLByReflection {
    public static void main(String[] args) throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName("JavaSparkSQL");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        SQLContext sqlCtx = new SQLContext(ctx);

        System.out.println("=== Data source: RDD ===");
        // Load a text file and convert each line to a Java Bean.
        JavaRDD<People> people = ctx.textFile("people.txt").map(
                new Function<String, People>() {
                    @Override
                    public People call(String line) {
                        String[] parts = line.split(",");

                        People people = new People();
                        people.setName(parts[0]);
                        people.setAge(Integer.parseInt(parts[1].trim()));
                        return people;
                    }
                });

        // Apply a schema to an RDD of Java Beans and register it as a table.
        DataFrame schemaPeople = sqlCtx.createDataFrame(people, People.class);
        schemaPeople.registerTempTable("people");

        // SQL can be run over RDDs that have been registered as tables.
        DataFrame teenagers = sqlCtx.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19");

        // The results of SQL queries are DataFrames and support all the normal RDD operations.
        // The columns of a row in the result can be accessed by ordinal.
        List<String> teenagerNames = teenagers.toJavaRDD().map(new Function<Row, String>() {
            @Override
            public String call(Row row) {
                return "Name: " + row.getString(0);
            }
        }).collect();

        for (String name : teenagerNames) {
            System.out.println(name);
        }


        System.out.println("=== Data source: Parquet File ===");
        // DataFrames can be saved as parquet files, maintaining the schema information.
        schemaPeople.saveAsParquetFile("people.parquet");

        // Read in the parquet file created above.
        // Parquet files are self-describing so the schema is preserved.
        // The result of loading a parquet file is also a DataFrame.
        DataFrame parquetFile = sqlCtx.parquetFile("people.parquet");

        //Parquet files can also be registered as tables and then used in SQL statements.
        parquetFile.registerTempTable("parquetFile");
        DataFrame teenagers2 =
                sqlCtx.sql("SELECT name FROM parquetFile WHERE age >= 13 AND age <= 19");
        teenagerNames = teenagers2.toJavaRDD().map(new Function<Row, String>() {
            @Override
            public String call(Row row) {
                return "Name: " + row.getString(0);
            }
        }).collect();
        for (String name : teenagerNames) {
            System.out.println(name);
        }

        System.out.println("=== Data source: JSON Dataset ===");
        // A JSON dataset is pointed by path.
        // The path can be either a single text file or a directory storing text files.
        String path = "people.json";
        // Create a DataFrame from the file(s) pointed by path
        DataFrame peopleFromJsonFile = sqlCtx.jsonFile(path);

        // Because the schema of a JSON dataset is automatically inferred, to write queries,
        // it is better to take a look at what is the schema.
        peopleFromJsonFile.printSchema();
        // The schema of people is ...
        // root
        //  |-- age: IntegerType
        //  |-- name: StringType

        // Register this DataFrame as a table.
        peopleFromJsonFile.registerTempTable("people");

        // SQL statements can be run by using the sql methods provided by sqlCtx.
        DataFrame teenagers3 = sqlCtx.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19");

        // The results of SQL queries are DataFrame and support all the normal RDD operations.
        // The columns of a row in the result can be accessed by ordinal.
        teenagerNames = teenagers3.toJavaRDD().map(new Function<Row, String>() {
            @Override
            public String call(Row row) {
                return "Name: " + row.getString(0);
            }
        }).collect();
        for (String name : teenagerNames) {
            System.out.println(name);
        }

        // Alternatively, a DataFrame can be created for a JSON dataset represented by
        // a RDD[String] storing one JSON object per string.
        List<String> jsonData = Arrays.asList(
                "{\"name\":\"Yin\",\"address\":{\"city\":\"Columbus\",\"state\":\"Ohio\"}}");
        JavaRDD<String> anotherPeopleRDD = ctx.parallelize(jsonData);
        DataFrame peopleFromJsonRDD = sqlCtx.jsonRDD(anotherPeopleRDD.rdd());


        // Take a look at the schema of this new DataFrame.
        peopleFromJsonRDD.printSchema();
        // The schema of anotherPeople is ...
        // root
        //  |-- address: StructType
        //  |    |-- city: StringType
        //  |    |-- state: StringType
        //  |-- name: StringType

        peopleFromJsonRDD.registerTempTable("people2");

        DataFrame peopleWithCity = sqlCtx.sql("SELECT name, address.city FROM people2");
        List<String> nameAndCity = peopleWithCity.toJavaRDD().map(new Function<Row, String>() {
            @Override
            public String call(Row row) {
                return "Name: " + row.getString(0) + ", City: " + row.getString(1);
            }
        }).collect();
        for (String name : nameAndCity) {
            System.out.println(name);
        }

        ctx.stop();
    }

    public static class People implements Serializable {
        private String name;
        private int age;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }
    }
}