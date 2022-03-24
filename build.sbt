ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

val sparkVersion = "3.2.1"

scalacOptions in(Compile, doc) ++= Seq("-groups", "-implicits", "-deprecation", "-Ywarn-dead-code", "-Ywarn-value-discard", "-Ywarn-unused")

unmanagedBase := baseDirectory.value / "spark-csv/lib"

parallelExecution in Test := false

name := "AnalyzingMovieRating"
libraryDependencies ++= Seq(
	"org.apache.spark" %% "spark-core" % sparkVersion,
	"org.apache.spark" %% "spark-sql" % sparkVersion,
)
libraryDependencies += "org.scalactic" %% "scalactic" % "3.2.11"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.11" % "test"
libraryDependencies += "org.scalatest" %% "scalatest-flatspec" % "3.2.11" % "test"
