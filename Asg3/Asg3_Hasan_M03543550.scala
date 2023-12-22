// Databricks notebook source
// Load the dataset from movies.csv to df1
val df1 = spark.read.option("header", "true")
                    .option("inferSchema","true")
                    .csv("/FileStore/tables/movies.csv")

// Ensure that the data has been uploaded successfully
df1.show(2)

// Register the DataFrame df1 as an SQL table "movies_table"
df1.createOrReplaceTempView("movies_table")

// Load the dataset from movies_ratings to df2
val df2 = spark.read.option("header", "true")
                    .option("inferSchema","true")
                    .csv("/FileStore/tables/movie_ratings.csv")

// Ensure that the data has been uploaded successfully
df2.show(2)

// Register the DataFrame df2 as an SQL table "movie_reviews_table"
df2.createOrReplaceTempView("movie_reviews_table")

// COMMAND ----------

/*
Write DataFrame-based Spark code to find the number of distinct movies in the file movies.csv
*/

import org.apache.spark.sql.functions.countDistinct

// Count distinct movie titles with renaming
val distinctMoviesCountDataFrameWay = df1.select(countDistinct("title").as("Distinct Movies Count"))

distinctMoviesCountDataFrameWay.show

// COMMAND ----------

/*
Write DataFrame-based Spark code to find the titles of the movies that appear in
the file movies.csv but do not have a rating in the file movie_ratings.csv. Remark: the answer
could be empty.
*/

// Expression to join both data frame based on similar title
var joinExpression = df1.col("title") === df2.col("title")

/*
* Steps:
* 1. Left anti join: Keeps rows from movies.csv (df1) which does not match join expression
* 2. Select only title column
* 3. Drop duplicate title values if any
*/
val moviesOnlyInMoviesCsvDataFrameWay = df1.join(df2, joinExpression, "left_anti")
                                           .select("title")
                                           .dropDuplicates("title")

moviesOnlyInMoviesCsvDataFrameWay.show

// COMMAND ----------

/*
Write DataFrame-based Spark code to find the number of movies that appear in the
ratings file (i.e., movie_ratings.csv) but not in the movies file (i.e., movies.csv).
*/

import org.apache.spark.sql.functions.countDistinct

// Expression to join both data frame based on similar title
var joinExpression = df1.col("title") === df2.col("title")

/*
* Steps:
* 1. Left anti join: Keeps rows from movie_ratings.csv (df2) which does not match join expression
* 2. Count distinct values of title column
*/
var moviesOnlyInMovieRatingsCsvDataFrameWay = df2.join(df1, joinExpression, "left_anti")
                                                 .select(countDistinct("title"))

moviesOnlyInMovieRatingsCsvDataFrameWay.show

// COMMAND ----------

/*
Write DataFrame-based Spark code to find the total number of distinct movies that
appear in either movies.csv, or movie_ratings.csv, or both.
*/

import org.apache.spark.sql.functions.countDistinct

/*
* Steps:
* 1. Union only title column from both movies.csv and movie_ratings.csv
* 2. Count distinct values from the union-ed title column
*/
val totalMoviesInBothTableDrameWay = df1.select("title")
                                        .union(df2.select("title"))
                                        .select(countDistinct("title"))

totalMoviesInBothTableDrameWay.show

// COMMAND ----------

/*
Write DataFrame-based Spark code to find the title and year for movies that were
remade. These movies appear more than once in the ratings file with the same title but
different years. Sort the output by title.
*/

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.count

// Create a window specification partioned by title column
var windowSpec = Window.partitionBy("title")

/*
* Steps:
* 1. Applying count aggregator per partition using window specification
* 2. Filtering out movies that were remade more than 1 year
* 3. Select the asked columns, title and year with ascending order by title
*/
val remadeMoviesDataFrameWay = df2.withColumn("count", count("year").over(windowSpec))
                                  .where("count > 1")
                                  .select("title", "year")
                                  .orderBy('title.asc)

remadeMoviesDataFrameWay.show(false)

// COMMAND ----------

/*
Write DataFrame-based Spark code to find the rating for every movie that the actor
"Branson, Richard" appeared in. Schema of the output should be (title, year, rating)
*/

// Create a sequence to join on both title and year
var joinSequnce = Seq("title", "year")

/*
* Steps:
* 1. Inner join the two data frames based on both similar title and year to match appearance
* 2. Filter out the actor "Branson, Richard" and select the asked columns
*/
val movieRatingOfBransonDataFrameWay = df1.join(df2, joinSequnce, "inner")
                                          .where("actor = 'Branson, Richard'")
                                          .select("title", "year", "rating")

movieRatingOfBransonDataFrameWay.show

// COMMAND ----------

/*
Write DataFrame-based Spark code to find the highest-rated movie per year and
include all the actors in that movie. The output should have only one movie per year, and it
should contain four columns: year, movie title, rating, and a list of actor names. Sort the output
by year.
*/

import org.apache.spark.sql.functions.{rank, collect_list}

// Window specification to get descending rating per year partition
var windowSpec = Window.partitionBy("year").orderBy('rating.desc)

// Create a sequence to join on both title and year
var joinSequence = Seq("title", "year")

// Applying the rank function using window specification on df2 (movie_ratings.csv) to rank the order.
// And filter out the top rated movie per year
val highestRatedMoviePerYearDataFrameWay = df2.withColumn("rank", rank.over(windowSpec))
                                              .where("rank = 1")

/*
* Steps:
* 1. Left outer join the calculated top rated per year data frame with the movies.csv dataframe
* 2. Group by the set (year, title, rating) and aggregate actors list on the actors columns
* 3. Sort the output by year on ascending order
*/
val highestRatedMoviePerYearWithAcotsDataFrameWay = highestRatedMoviePerYearDataFrameWay
                                                    .join(df1, joinSequence, "left_outer")
                                                    .groupBy("year", "title", "rating")
                                                    .agg(collect_list("actor").as("actors"))
                                                    .orderBy('year.asc)

highestRatedMoviePerYearWithAcotsDataFrameWay.show(false)

// COMMAND ----------

/*
Write DataFrame-based Spark code to determine which pair of actors worked
together most. Working together is defined as appearing in the same movie. The output should
have three columns: actor 1, actor 2, and count. The output should be sorted by the count in
descending order.
*/
import org.apache.spark.sql.functions.rank

// Window specification on the whole data set with ascending order by actor
val windowSpec = Window.orderBy('actor.asc)

// Apply rank on the actor column to assign unique number to per unique actor
val df1WithActorRanked = df1.withColumn("rank", rank.over(windowSpec))

// Create two data frames from the ranked data frame by renaming the actor and rank column to specific data frames
val df1ForActor1 = df1WithActorRanked.withColumnRenamed("actor", "actor 1").withColumnRenamed("rank", "actor 1 rank")
val df1ForActor2 = df1WithActorRanked.withColumnRenamed("actor", "actor 2").withColumnRenamed("rank", "actor 2 rank")

// A join expression to join two data frames based on both similar title and year, 
// and for per actor, join with greater ranked actors so that we would not get duplicate (actor 1, actor 2) sets
var joinExpression = df1ForActor1.col("title") === df1ForActor2.col("title") &&
                     df1ForActor1.col("year") === df1ForActor2.col("year") &&
                     df1ForActor1.col("actor 1 rank") < df1ForActor2.col("actor 2 rank")

// 1. Join based on the join expression, 2. Group by (actor 1, actor 2) sets and count, and 3. Descending order by count
var actorsWorkedTogetherDataFrameWay = df1ForActor1.join(df1ForActor2, joinExpression)
                                                   .groupBy("actor 1", "actor 2")
                                                   .count()
                                                   .orderBy('count.desc)

actorsWorkedTogetherDataFrameWay.show(false)
