import pyspark.sql.types as T
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession
import datetime

spark: SparkSession = SparkSession.builder.master("local").\
    appName("Home_work").\
    config("spark.driver.bindAddress", "localhost").\
    config("spark.ui.port", "4040").\
    getOrCreate()

url = ""
schema_name="public"
creds = {
    "user": "",
    "password": "",
    "driver": "",
}


def get_table_conn(table_name):
    full_table_name = schema_name + table_name
    return spark.read.jdbc(
        url,
        full_table_name,
        properties=creds,
    )


schema_web = T.StructType([
    T.StructField("id", T.StringType(), True),
    T.StructField("timestamp", T.LongType(), True),
    T.StructField("type", T.StringType(), True),
    T.StructField("page_id", T.IntegerType(), True),
    T.StructField("tag", T.StringType(), True),
    T.StructField("sign", T.BooleanType(), True),
])

schema_lk = T.StructType([
    T.StructField("id", T.StringType(), True),
    T.StructField("user_id", T.IntegerType(), True),
    T.StructField("fio", T.StringType(), True),
    T.StructField("dob", T.DateType(), True),
    T.StructField("doc", T.DateType(), True),
])

data_web = [
    (1, 1667627426, "visit", 101, 'Sport', False),
    (1, 1667627486, "scroll", 101, 'Sport', False),
    (1, 1667627500, "click", 101, 'Sport', False),
    (1, 1667627505, "visit", 102, 'Politics', False),
    (1, 1667627565, "click", 102, 'Politics', False),
    (1, 1667627586, "visit", 103, 'Sport', False),
    (2, 1667628001, "visit", 104, 'Politics', True),
    (2, 1667628101, "scroll", 104, 'Politics', True),
    (2, 1667628151, "click", 104, 'Politics', True),
    (2, 1667628200, "visit", 105, 'Business', True),
    (2, 1667628226, "click", 105, 'Business', True),
    (2, 1667628317, "visit", 106, 'Business', True),
    (2, 1667628359, "scroll", 106, 'Business', True),
    (3, 1667628422, "visit", 101, 'Sport', False),
    (3, 1667628486, "scroll", 101, 'Sport', False),
    (4, 1667628505, "visit", 106, 'Business', False),
    (5, 1667628511, "visit", 101, 'Sport', True),
    (5, 1667628901, "click", 101, 'Sport', True),
    (5, 1667628926, "visit", 102, 'Politics', True),
    (5, 1667628976, "click", 102, 'Politics', True),
]

data_lk = [
    (101, 2, "Иванов Иван Иванович", datetime.datetime(1990, 7, 5), datetime.datetime(2016, 8, 1)),
    (102, 5, "Александрова Александра александровна", datetime.datetime(1995, 1, 22), datetime.datetime(2017, 10, 7)),
]

df_web: DataFrame = spark.createDataFrame(data=data_web, schema=schema_web)
df_lk: DataFrame = spark.createDataFrame(data=data_lk, schema=schema_lk)

df_web_1 = df_web.select(F.from_unixtime("timestamp").alias("event_time"))
df_web_1.show(5)

df_web.groupby("id")\
    .count()\
    .orderBy("count", ascending=False)\
    .show(10)
df_web.groupby("id").agg(
    {
        "type": "count",
        "page_id": "max",
    }
).show(10)

df_web_1.withColumn("new", F.floor(F.hour("event_time")/F.lit(4))).show(5)

joined = df_lk.alias("lk").join(
    df_web.alias("web"),
    on=[F.col("lk.user_id") == F.col("web.id")],
    how="inner"
)
joined.show()

@F.udf(returnType=T.StringType())
def gender(fio: str):
    surname, name, middlename = fio.split(' ')
    if ((surname[-2:] == "ов" or surname[-2:] == "ев") and
        (middlename[-2:] == "ич")):
        return "m"
    else:
        return "w"

joined.withColumn("gender", gender(F.col("fio"))).select("page_id", "gender")\
    .groupby("page_id", "gender")\
    .count()\
    .withColumn(
        "max_cnt",
        F.max(F.col("count"))\
        .over(
            Window.partitionBy("gender")
        )
    )\
    .show()