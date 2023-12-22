// Databricks notebook source
// Load the dataset
val df = spark.read.option("header", "true")
                   .option("inferSchema","true")
                   .csv("/FileStore/tables/movies.csv")

// Ensure that the data has been uploaded successfully
df.show(false)
df.count

// Register the DataFrame as an SQL table
df.createOrReplaceTempView("movies_table")

// COMMAND ----------

/*
Write an SQL-based Spark code to compute the number of movies produced in each year. 
The output should have two columns for year and count. 
The output should be ordered in ascending order by year. 
It is alright that Spark will only display the first 20 rows.
*/

/** 
* Steps: 
* 1. group by year 
* 2. aggregate year-wise count and rename count column
* 3. filter out year having null value
* 4. ascending order by year
*/
var moviesCountPerYearSqlWay = spark.sql(
  """
  select year, count(title) as count 
  from movies_table 
  group by year
  having year is not null
  order by year asc
  """
)

moviesCountPerYearSqlWay.show(false)

// COMMAND ----------

/*
Write DataFrame-based Spark code to do the same thing as in the previous cell.
*/

import org.apache.spark.sql.functions.count

/** 
* Steps: 
* 1. group by year 
* 2. aggregate year-wise count and rename count column
* 3. filter out year having null value
* 4. ascending order by year
*/
var moviesCountPerYearDataFrameWay = df.groupBy("year")
                                       .agg(count("*").as("count"))
                                       .where("year is not null")
                                       .orderBy('year.asc)

moviesCountPerYearDataFrameWay.show

// COMMAND ----------

/*
Write an SQL-based Spark code to find the five top most actors who acted in
the most number of movies. Schema of the output must be (actor, number_of_movies),
or in other words, rename the column with the count as number_of_movies.
*/

/**
* Steps: 
* 1. group by actor 
* 2. aggregate actor-wise movies count and rename the count column 
* 3. descending order by number of movies
* 4. taking top 5 rows
*/
var top5ActorsInNumberOfMoviesSqlWay = spark.sql(
  """
  select actor, count(title) as number_of_movies 
  from movies_table 
  group by actor 
  order by number_of_movies desc 
  limit 5
  """
)

top5ActorsInNumberOfMoviesSqlWay.show

// COMMAND ----------

/*
Write DataFrame-based Spark code to do the same thing as in the previous
cell. Make sure that schema of the output is (actor, number_of_movies).
*/

/**
* Steps: 
* 1. group by actor 
* 2. aggregate actor-wise movies count and rename the count column 
* 3. descending order by number of movies 
* 4. taking top 5 rows (here, limit method is a transformation)
*/
var top5ActorsInNumberOfMoviesDataFrameWay = df.groupBy("actor")
                                               .agg(count("title").as("number_of_movies"))
                                               .orderBy('number_of_movies.desc)
                                               .limit(5)

top5ActorsInNumberOfMoviesDataFrameWay.show

// COMMAND ----------

/*
Write DataFrame-based Spark code to find the title and year for every movie
that Tom Hanks acted in (the name is stored as Hanks, Tom in the csv file). Make sure
that the output is sorted in ascending order by year. Notice that schema of the output
must be (title, year). This means that actor name should not be part of the output.
*/

/**
* Steps:
* 1. select title and year column
* 2. filter data-set having 'Hanks, Tom' as an actor
* 3. ascending order by year
*/
var tomHanksMoviesDataFrameWay = df.select("title", "year").where("actor == 'Hanks, Tom'").orderBy('year.asc)

tomHanksMoviesDataFrameWay.show
