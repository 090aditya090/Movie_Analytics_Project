#                                           Spark Data Frames
#==============================================================================================================================
# 1. Prepare Movies data: Extracting the Year and Genre from the Text
# 2. Prepare Users data: Loading a double delimited csv file
# 3. Prepare Ratings data: Programmatically specifying a schema for the data frame
# 4. Import Data from URL: Scala
# 5. Save table without defining DDL in Hive
# 6. Broadcast Variable example
# 7. Accumulator example

#=============================================================================================================================
#=============================================================================================================================

# Import required libraries
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import split, trim, substring, regexp_extract

# Create a SparkSession
spark = SparkSession.builder.appName("MovieLens Analysis").getOrCreate()

# Define the schema for each DataFrame
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


#===============================================================================================================================

# 1. Prepare Movies data: Extracting the Year and Genre from the Text.
#---------------------------------------------------------------------

# Read the data from the movies.dat using the spark.read method:
movies_df = spark.read.format("csv") \
          .option("delimiter", "::") \
          .schema(movies_schema) \
          .load("/movieData/dataset/movies.dat")

print("1. Prepare Movies data: Extracting the Year and Genre from the Text.")

# Extract the year as column "year" from the "movie" field using a regular expression
movies_df = movies_df.withColumn("year", regexp_extract(movies_df["title"], r"\((\d{4})\)", 1))

# Extract the genre(s) from the "genres" field using the split function
movies_df = movies_df.withColumn("genres", split("genres", "\|"))

# Show Data Frame
movies_df.show()

#===============================================================================================================================

# 2. Prepare Users data: Loading a double delimited csv file.
#------------------------------------------------------------

print("2. Prepare Users data: Loading a double delimited csv file.")

users_df = spark.read.format("csv") \
          .option("delimiter", "::") \
          .schema(users_schema) \
          .load("/movieData/dataset/users.dat")

# Show dataframes
users_df.show()

#=================================================================================================================================

# 3. Prepare Ratings data: Programmatically specifying a schema for the data frame.
#----------------------------------------------------------------------------------
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType

# define schema
ratings_schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("movie_id", IntegerType(), True),
    StructField("rating", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

print("3. Prepare Ratings data: Programmatically specifying a schema for the data frame.")

ratings_df = spark.read.format("csv") \
          .option("delimiter", "::") \
          .schema(ratings_schema) \
          .load("/movieData/dataset/ratings.dat")

# show ratings_df data frame
ratings_df.show()

#================================================================================================================================

#4. Import data from a URL using Scala code
#------------------------------------------

# scala_code = """
# import org.apache.spark.sql.functions._
# val url = "http://example.com/data.csv"
# val df = spark.read.format("csv").option("header", true).load(url)
# df.show()
# """
# spark.sparkContext.addFile("http://example.com/example.scala")
# spark.sparkContext.setCheckpointDir("/tmp")
# spark.sparkContext.runJob(spark.sparkContext.parallelize([1]), lambda x: exec(scala_code))