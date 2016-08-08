
name := "assignments"

version := "1.0"

scalaVersion := "2.11.7"

organization := "com.knoldus"

libraryDependencies ++= Seq("org.apache.spark" % "spark-core_2.11" % "2.0.0", "org.apache.spark" % "spark-sql_2.11" % "2.0.0")


ivyScala := ivyScala.value map {
  _.copy(overrideScalaVersion = true)
}
fork in run := true
