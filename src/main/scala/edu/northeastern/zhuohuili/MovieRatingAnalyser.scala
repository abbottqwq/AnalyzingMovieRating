package edu.northeastern.zhuohuili

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.language.postfixOps

case class MovieRatingAnalyser(resource: String) {
	val spark = SparkSession.builder()
		.appName("Analyzing Movie Rating")
		.config("spark.master", "local[*]")
		.getOrCreate()

	spark.sparkContext.setLogLevel("ERROR")

	val moviesDF = spark.read
		.format("csv")
		.options(Map(
			"mode" -> "failFast",
			"path" -> s"$resource",
			"header" -> "true",
			"inferSchema" -> "true"
		))
		.load()

	def getAvg(column: String) = moviesDF.select(avg(column))
	def getStddev(column: String) = moviesDF.select(stddev(column))
}

object MovieRatingAnalyser extends App {
	def apply(resource: String): MovieRatingAnalyser = new MovieRatingAnalyser(resource)

	val ma = apply("src/main/resources/movie_metadata.csv") // from the one in the repository: https://github.com/rchillyard/CSYE7200/blob/Spring2022/spark-csv/src/main/resources/movie_metadata.csv
	ma.getAvg("imdb_score").show()
	ma.getStddev("imdb_score").show()
}
