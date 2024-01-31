ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.10"

lazy val root = (project in file("."))
  .settings(
    name := "Spark_assign1"
  )
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.0"
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.0"
