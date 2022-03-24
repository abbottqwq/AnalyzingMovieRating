package edu.northeastern.zhuohuili

import org.scalatest.BeforeAndAfter
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec

object MovieRatingAnalyserTester extends AnyFlatSpec with Matchers with BeforeAndAfter {
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
		m.moviesDF.count() should matchPattern { case a: Int if a > 0 => }
		m.moviesDF.show()
	}

}
