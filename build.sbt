name := "tdt4305-bigdata-architecture-project"

version := "0.1"

scalaVersion := "2.11.12"
val sparkVersion = "2.4.5"


libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion

mainClass in (Compile, packageBin) := Some("sentiment_analysis")

