lazy val root = (project in file(".")).
  settings(
    name := "Inopco_CCN_Main_Revenue",
    version := "0.1",
    scalaVersion := "2.11.8",
    mainClass in assembly := Some("Driver.fact_event_load")
  )

resolvers += "Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies ++= {
  Seq(

    /** Spark Dependencies */
    "org.apache.spark" %% "spark-core" % "2.2.0",
    "org.apache.spark" %% "spark-sql" % "2.2.0",
    "com.databricks" %% "spark-csv" % "1.5.0",
    /**hadoop Dependencies*/
    "org.apache.hadoop" % "hadoop-hdfs" % "2.7.3",
    "org.apache.hadoop" % "hadoop-common" % "2.7.3" %"provided",
    "org.apache.hadoop" % "hadoop-client" % "2.7.3",
    "commons-io" % "commons-io" % "2.4"
  )}

// META-INF discarding
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case PathList("reference.conf") => MergeStrategy.concat
  case x => MergeStrategy.first
}