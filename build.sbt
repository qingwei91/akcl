ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

lazy val root = (project in file("."))
  .settings(
    name         := "akcl",
    organization := "com.github.qingwei91",
    libraryDependencies ++= Seq(
      "org.apache.kafka"         % "kafka-clients"   % "3.3.2" % Provided,
      "com.disneystreaming"     %% "weaver-cats"     % "0.8.1" % Test,
      "io.github.embeddedkafka" %% "embedded-kafka"  % "3.3.2" % Test,
      "ch.qos.logback"           % "logback-classic" % "1.4.5" % Test
    ),
    javacOptions ++= Seq("-source", "11"),
    testFrameworks += new TestFramework("weaver.framework.CatsEffect")
  )
