package Assignment

import org.apache.spark.sql.SparkSession

object MovieRatingAnalyser extends App {
	val spark = SparkSession.builder()
		.appName("Analyzing Movie Rating")
		.config("spark.master", "local")
		.getOrCreate()

	spark.sparkContext.setLogLevel("ERROR")

	val movieDF = spark.read
		.format("csv")
		.options(Map(
			"mode" -> "failFast",
			"path" -> "src/main/Resources/movie_metadata.csv",
			"header" -> "true",
			"inferSchema" -> "true"
		))
		.load()

	movieDF.printSchema()
	movieDF.show()
}
