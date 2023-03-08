#								                SparkSQL
#===================================================================================================================================================
#1. Create tables for movies.dat, users.dat and ratings.dat: Saving Tables from Spark SQL
#2. Find the list of the oldest released movies.
#3. How many movies are released each year?
#4. How many number of movies are there for each rating?
#5. How many users have rated each movie?
#===================================================================================================================================================
#===================================================================================================================================================

#1. Create tables for movies.dat, users.dat and ratings.dat: Saving Tables from Spark SQL
#----------------------------------------------------------------------------------------
#To create tables for movies.dat, users.dat, and ratings.dat in Spark SQL, you can follow these steps:
	
#1. Create a SparkSession object:

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("CreateTablesExample").getOrCreate()

#2. Define the schema for each DataFrame using the StructType and StructField classes:

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



#4. Create temporary tables for each DataFrame using the "createOrReplaceTempView" method:

# Registering the temporary tables
movies_df.createOrReplaceTempView("movies")
ratings_df.createOrReplaceTempView("rating")
users_df.createOrReplaceTempView("user")

spark.sql("select * from movies").show(10)
spark.sql("select * from rating").show(10)
spark.sql("select * from user").show(10)

#5. Save the DataFrames as permanent tables in a Hive metastore using the saveAsTable() function.

# Saving the tables as permanent tables in Hive metastore
movies_df.write.saveAsTable("movies_table")
users_df.write.saveAsTable("users_table")
ratings_df.write.saveAsTable("ratings_table")





	   