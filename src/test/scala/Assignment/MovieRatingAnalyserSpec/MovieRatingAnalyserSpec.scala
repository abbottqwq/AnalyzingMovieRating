package MovieRatingAnalyserSpec

import Assignment.MovieRatingAnalyser
import org.scalatest.{BeforeAndAfter, flatspec}
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.SparkSession

object MovieRatingAnalyserSpec extends flatspec.AnyFlatSpec with Matchers with BeforeAndAfter {
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
	}



}
