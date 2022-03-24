package edu.northeastern.zhuohuili

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class MovieRatingAnalyserTester extends AnyFlatSpec with Matchers with BeforeAndAfter {
	implicit var spark: SparkSession = _

	before {
		spark = SparkSession
			.builder()
			.appName("Analyzing Movie Rating")
			.master("local[*]")
			.getOrCreate()
		spark.sparkContext.setLogLevel("ERROR")

	}
	after {
		if (spark != null) {
			spark.stop()
		}
	}

	behavior of "reading"

	it should "read data from csv file correctly" in {
		val m = MovieRatingAnalyser("src/main/resources/movie_metadata.csv")
		m.moviesDF.count() should matchPattern { case a: Long if a > 0 => }
	}


	it should "right mean and std" in {
		val m = MovieRatingAnalyser("src/main/resources/movie_metadata.csv")
		m.getAvg("imdb_score").show()
		m.getAvg("imdb_score").first().getDouble(0) shouldBe 6.45 +- 0.01
		m.getStddev("imdb_score").first().getDouble(0) shouldBe 0.99 +- 0.01
	}

}
