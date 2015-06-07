name := "DataMig"

version := "0.13.8"

scalaVersion := "2.11.6"

libraryDependencies ++= Seq(
  "org.scalatest" % "scalatest_2.11" % "2.2.4" % "test",
  "org.apache.spark" %% "spark-core" % "1.3.0",
  //"net.liftweb" %% "lift-json" % "2.6+",
  "com.github.nscala-time" %% "nscala-time" % "2.0.0",
  "com.typesafe.play" %% "play-json" % "2.4.0"

)