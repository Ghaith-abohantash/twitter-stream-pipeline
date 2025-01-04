ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.12.10"
val sparkVersion = "3.5.3"

lazy val root = (project in file("."))
  .settings(
    name := "twitter-stream-pipeline",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-mllib" % sparkVersion,
      "org.apache.kafka" % "kafka-clients" % "3.0.0",
      "org.scala-lang" % "scala-library" % "2.12.10",
      "org.json4s" %% "json4s-native" % "3.6.6",
      "org.json4s" %% "json4s-ast" % "3.6.6",
      "org.json4s" %% "json4s-core" % "3.6.6",
      "org.json4s" %% "json4s-jackson" % "3.6.6",
      "org.json4s" %% "json4s-scalap" % "3.6.6",
      "com.johnsnowlabs.nlp" %% "spark-nlp" % "5.0.0",
      "org.apache.hadoop" % "hadoop-client" % "3.3.1",
      "org.apache.parquet" % "parquet-hadoop" % "1.11.1",
      "com.typesafe.play" %% "play-json" % "2.9.2",
      "org.apache.httpcomponents" % "httpclient" % "4.5.13",
"org.elasticsearch.client" % "elasticsearch-rest-high-level-client" % "7.10.0"
    )
  )
