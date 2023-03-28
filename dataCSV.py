from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CreateTablesExample").getOrCreate()

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

movies_schema = StructType([
    StructField("movie_id", IntegerType(), True),
    StructField("title", StringType(), True),
    StructField("genres", StringType(), True)
])

users_schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("gender", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("occupation", IntegerType(), True),
    StructField("zipcode", StringType(), True)
])

ratings_schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("movie_id", IntegerType(), True),
    StructField("rating", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])


#3. Read the data from the files using the spark.read method:

movies_df = spark.read.format("csv") \
          .option("delimiter", "::") \
          .schema(movies_schema) \
          .load("/movieData/dataset/movies.dat")

ratings_df = spark.read.format("csv") \
          .option("delimiter", "::") \
          .schema(ratings_schema) \
          .load("/movieData/dataset/ratings.dat")

users_df = spark.read.format("csv") \
          .option("delimiter", "::") \
          .schema(users_schema) \
          .load("/movieData/dataset/users.dat")

movies_df.show(10)
movies_df.write.csv("/output/movies.csv", header=True)

ratings_df.show(10)
ratings_df.write.csv("/output/ratings.csv", header=True)

users_df.show(10)
users_df.write.csv("/output/users.csv", header=True)
