name := "DataMig"

version := "0.13.8"

scalaVersion := "2.10.4"

ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.2.4" % "test",
  "com.github.nscala-time" %% "nscala-time" % "2.0.0",

  "net.liftweb" %% "lift-json" % "2.6+",

  ("org.apache.spark" %% "spark-core" % "1.3.0" % "provided").
    exclude("org.mortbay.jetty", "servlet-api").
    exclude("commons-beanutils", "commons-beanutils-core").
    exclude("commons-collections", "commons-collections").
    exclude("commons-logging", "commons-logging").
    exclude("com.esotericsoftware.minlog", "minlog")
)

assemblyMergeStrategy in assembly := {
  case PathList("javax", xs @ _*)         => MergeStrategy.first
  case PathList("org", "apache", "hadoop", "yarn", xs @ _*)         => MergeStrategy.first
  case PathList("org", "apache", "spark", "unused", xs @ _*)         => MergeStrategy.first
  case PathList("com", "google", "common" , "base", xs @ _*)        => MergeStrategy.first

  case "plugin.properties" => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

test in assembly := {}
// can only have one Spark context and each test creates one so they can be isolated so execute tests serially
parallelExecution in Test := false
