ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"

val scalaTestVersion = "3.2.11"

val sparkVersion = "3.2.1"

lazy val root = (project in file("."))
	.settings(
		name := "AnalyzingMovieRating"
	).settings(
	libraryDependencies += "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
	libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion,
	libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
)
