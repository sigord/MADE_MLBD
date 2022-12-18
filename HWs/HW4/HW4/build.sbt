ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.12"

val sparkVersion = "3.0.1"

lazy val root = (project in file("."))
  .settings(
    name := "HW4"
  )

ThisBuild / libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion withSources(),
  "org.apache.spark" %% "spark-mllib" % sparkVersion withSources()
)

ThisBuild / libraryDependencies += ("org.scalatest" %% "scalatest" % "3.2.2" % "test" withSources())