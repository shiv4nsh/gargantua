
name := "assign1"

version := "1.0"

scalaVersion := "2.11.7"

organization := "com.knoldus"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.0.0-preview"


ivyScala := ivyScala.value map {
  _.copy(overrideScalaVersion = true)
}
fork in run := true
