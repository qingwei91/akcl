ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

lazy val root = (project in file("."))
  .settings(
    name := "akcl",
    libraryDependencies ++= Seq(
      "org.apache.kafka" % "kafka-clients" % "3.4.0"
    )
  )
