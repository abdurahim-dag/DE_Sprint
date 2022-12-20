// Практика.
// 3.3 Команды Spark

import org.apache.spark.sql.{Row, SparkSession, types}
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructType, BooleanType, StructField, DateType}
import org.apache.spark.sql.functions.{array_contains, col, lit, max, min, sum, udf, when, desc, floor, date_trunc, to_timestamp, hour}

object App extends App{
  var spark: SparkSession = SparkSession.builder
    .master("local[*]")
    .appName("Spark Word Count")
    .config("spark.executor.instances", "4")
    .config("spark.executor.memory", "4g")
    .config("spark.executor.cores", "4")
    .getOrCreate()

  // datediff
  // concurrent_date
  // floor - round
  // a.
  val schemaStruct = StructType(
    StructField("id", IntegerType, true) ::
    // |LongType|
    StructField("timestamp", IntegerType, true) ::
    StructField("type", StringType, true) ::
    StructField("page_id", IntegerType, true) ::
    StructField("tag", StringType, true) ::
    StructField("sign", BooleanType, true) :: Nil)
  // b, c.
  val df =spark.read
    .option("delimiter",",")
    .option("header", "false")
    .schema(schemaStruct)
    .csv("./src/main/scala/data.csv")

  // d.
  // Вывести топ-5 самых активных посетителей сайта
  var df1 = df.groupBy("id")
    .count().as("count")
    .orderBy(desc("count"))
  df1.show()
  //df1.write.option("header", "true").mode("append").csv("result")

  // Посчитать процент посетителей, у которых есть ЛК
  df1 = df
    .select("id", "sign")
    .dropDuplicates()
    .groupBy("sign")
    .count()
    .withColumn("signed", when(col("sign")==="true", col("count"))
      .otherwise(0))
    .agg(
      sum("signed").as("signed"),
      sum("count").as("count")
    ).select("signed", "count")
    .withColumn(
      "percent signed",
      col("signed") / col("count") * 100
    ).select(floor("percent signed"))
  df1.show()

  // Вывести топ-5 страниц сайта по показателю общего кол-ва кликов на данной странице
  df1 = df
    .where(col("type") === "click")
    .groupBy("page_id")
    .count()
    .orderBy(desc("count"))
  df1.show(5)

  //  Добавьте столбец к фрейму данных со значением временного диапазона в рамках суток с размером окна – 4 часа(0-4, 4-8, 8-12 и т.д.)
  df1 = df
    .withColumn("hour", hour(to_timestamp(col("timestamp"))))
    .withColumn("hours",
      when(col("hour") >= 0 && col("hour") < 4, "0-4")
      .when(col("hour") >= 4 && col("hour") < 8, "4-8")
      .when(col("hour") >= 8 && col("hour") < 12, "8-12")
      .when(col("hour") >= 12 && col("hour") < 16, "12-16")
      .when(col("hour") >= 16 && col("hour") < 20, "16-20")
      .when(col("hour") >= 20, "20-0")
    )
  df1.show()

  //  Выведите временной промежуток на основе предыдущего задания, в течение которого было больше всего активностей на сайте.
  df1.groupBy("hours")
    .count()
    .orderBy(desc("count")).show(1)

  // Создайте второй фрейм данных, который будет содержать информацию о ЛК посетителя сайта со следующим списком атрибутов
  val schemaStruct2 = StructType(
    StructField("id", IntegerType, true) ::
      StructField("user_id", IntegerType, true) ::
      StructField("full_name", StringType, true) ::
      StructField("birthday", DateType, true) ::
      StructField("date_in", DateType, true) :: Nil)
  val df2 = spark
    .read
    .format("csv")
    .option("dateFormat", "dd.MM.yyyy")
    .option("delimiter", ",")
    .option("header", "false")
    .schema(schemaStruct2)
    .load("./src/main/scala/user_data.csv")
  df2.show()

  // Вывести фамилии посетителей, которые читали хотя бы одну новость про спорт.
  df1.join(
    df2, df1("id") === df2("user_id")
  )
    .where(col("tag") === "Sport")
    .select("full_name")
    .show()
}
