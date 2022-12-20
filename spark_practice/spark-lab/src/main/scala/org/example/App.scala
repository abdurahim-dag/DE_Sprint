package org.example
import org.apache.spark.sql.{SparkSession, SaveMode}
import java.util.Properties
import org.apache.spark.sql.functions._

object SparkSessionTest extends App{
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExample")
      .getOrCreate();

    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost:35432/movies_database")
      .option("dbtable", "content.film_work")
      .option("user", "app")
      .option("password", "123qwe")
      .load()
    jdbcDF.show(10)

    val connectionProperties = new Properties()
    connectionProperties.put("user", "app")
    connectionProperties.put("password", "123qwe")

    // Specifying the custom data types of the read schema
    // connectionProperties.put("customSchema", "id DECIMAL(38, 0), name STRING")

    var jdbcDF1 = jdbcDF.filter("rating > 8.7")
    jdbcDF1 = jdbcDF1.withColumn("no_descr", when(col("description").isNull==="true", true))

    // Specifying create table column data types on write
    // .option("createTableColumnTypes", "name CHAR(64), comments VARCHAR(1024)")

    // Saving data to a JDBC source
    jdbcDF1.write
      .mode(SaveMode.Overwrite)
      .jdbc("jdbc:postgresql://localhost:35432/movies_database", "content.spark_test", connectionProperties)
}
