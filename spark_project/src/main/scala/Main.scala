import org.apache.spark.sql.{Row, SparkSession, types}
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructType}
import org.apache.spark.sql.functions.{array_contains, col, lit, max, min, sum, udf, when}

object Main extends App{

  def first_example(): Unit = {
    var spark: SparkSession = SparkSession.builder
    .master ("local[*]")
    .appName ("Spark Word Count")
    .config ("spark.executor.instances", "4")
    .config ("spark.executor.memory", "4g")
    .config ("spark.executor.cores", "4")
    .getOrCreate ()

    val lines = spark.sparkContext.parallelize (
    Seq ("Spark intelij idea Scala test one",
    "Spark intelij idea Scala test two",
    "Spark intelij idea Scala test three"
    )
    )
    val counts = lines
    .flatMap (line => line.split ("") )
    .map (word => (word, 1) )
    .reduceByKey (_+ _)

    counts.foreach (println)
  }

  def rdd_example(): Unit = {
    val spark: SparkSession = SparkSession.builder
      .master("local[1]")
      .appName("Spark RDD Example")
      .getOrCreate()
    val rdd = spark.sparkContext.parallelize(
      Seq(
        ("Java", 20000),
        ("Python", 40000),
        ("Scala", 30000)
      )
    )
    // val rdd = spark.sparkContext.textFile(path="/path/to/file.txt")
    rdd.foreach(println)
  }

  def rdd_to_df_example(): Unit = {
    val spark: SparkSession = SparkSession.builder
      .master("local[1]")
      .appName("Spark RDD Example")
      .getOrCreate()
    val rdd = spark.sparkContext.parallelize(
      Seq(
        ("Java", 20000),
        ("Python", 40000),
        ("Scala", 30000)
      )
    )
    import spark.implicits._
    val df = rdd.toDF(colNames = "Lang", "Users")
    df.printSchema()
  }

  def seq_to_df_exmaple(): Unit = {
    val spark: SparkSession = SparkSession.builder
      .master("local[1]")
      .appName("Spark RDD Example")
      .getOrCreate()
    import spark.implicits._

    val data = Seq(
        ("Java", "Sydney", 20000),
        ("Python", "NY", 40000),
        ("Scala", "Moscow", 30000)
    )
    val cols = Seq("Lang", "City", "Price")
    val df = data.toDF(cols:_*)
    //df.select("Lang", "Price").show()
    df.select(df.columns(2)).show()
  }

  def schema_exmaple(): Unit = {
    val spark: SparkSession = SparkSession.builder
      .master("local[1]")
      .appName("Spark RDD Example")
      .getOrCreate()
    import spark.implicits._


    val data = Seq(
      Row(Row("James", "Smiths"), List("Java", "Scala"), "Sydney", 20000),
      Row(Row("Anne", "Williams"), List("Python", "Ruby"), "NY", 40000),
      Row(Row("Robert", "Rose"), List("Scala", "Kotlin"), "Moscow", 30000)
    )
    val schema = new StructType()
      .add("name", new StructType()
        .add("firstname", StringType)
        .add("lastname", StringType))
      .add("job", ArrayType(StringType))
      .add("city", StringType)
      .add("price", IntegerType)

    var df = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      schema
    )

    df = df.withColumn("Country", lit("USA"))
      .withColumn("price+", col("price")*2)
      .withColumn("languages", col("job"))
      .withColumn("pricef", col("price").cast("Float"))
      .withColumnRenamed("city", "town")
      .drop("job")

    df.printSchema()
    df.show()
    df.filter(df("town") === "NY").show(truncate = false)
    df.where("town == 'NY'").show(truncate = false)
    df.filter(df("town") === "NY" || df("town") === "Moscow").show(truncate = false)
    df.filter(array_contains(df("languages"),"Python")).show(truncate = false)

    val df2 = df.withColumn("new_town", when(col("town")==="NY", "NYC")
      .when(col("town")==="Moscow", "Russian")
      .otherwise("Other"))
    df2.show()

    val df3 = df.withColumn("new_town",
      when(col("town") === "NY" || col("town") === "Moscow", "Big Capital")
      .otherwise("Other"))

    val df4 = df3.distinct()
    df4.show()
    val df5 = df3.dropDuplicates("Country")
    df5.show()

    var df6 = df3.groupBy("Country").count()
    df6.show()
    df6 = df3.groupBy("Country").sum("pricef")
    df6.show()

    df6 = df3.groupBy("Country")
      .agg(
        sum("pricef").as("sum"),
        min("pricef").as("min"),
        max("pricef").as("max")
      )
    df6.show()

    df3.orderBy("pricef", "town").show()
    df3.orderBy(col("pricef"), col("town")).show()

    df3.sort("pricef", "town").show()

    val columns = Seq("name", "population")
    val cities = Seq(
      ("Moscow", 30),
      ("NY", 40),
      ("Sydney", 15)
    )

    val df7 = cities.toDF(columns: _*)
    df7.join(
      df3, df3("town") === df7("name")
    ).show()

    //df7.union(df6).show()
    //df7.union(df6).distinct().show()


    // UserDefFunction
    val convertCase = (strQuote: String) => {
      val arr = strQuote.split(" ")
      arr.map(f => f.substring(0,1).toLowerCase + f.substring(1, f.length)).mkString(" ")
    }

    val convertUDF = udf(convertCase)
    df3.select(
      col("town"),
      convertUDF(col("town")).as("LowerCaseTown")
    ).show()

  }

  schema_exmaple()
}
