ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.12.10"

lazy val root = (project in file("."))
  .settings(
    name := "twitter-stream-pipeline",
    libraryDependencies ++= Seq(
      "org.apache.kafka" % "kafka-clients" % "3.8.0",
      "org.scala-lang" % "scala-library" % "2.12.10",
      "org.json4s" %% "json4s-native" % "4.0.3",
      "org.json4s" %% "json4s-jackson" % "4.0.3",
      "com.typesafe.play" %% "play-json" % "2.9.2",
      "org.apache.spark" %% "spark-core" % "3.5.2",
      "org.apache.spark" %% "spark-sql" % "3.5.2",
      "org.apache.spark" %% "spark-mllib" % "3.5.2",
      "com.johnsnowlabs.nlp" %% "spark-nlp" % "5.1.0",
      "org.apache.spark" %% "spark-sql" % "3.5.2"
    )
  )