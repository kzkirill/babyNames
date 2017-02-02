import AssemblyKeys._

name := "Sparked Baby Names"

version := "1.0"

scalaVersion := "2.11.8"

val sparkVersion: String = "2.1.0"

val hiveVersion:String = "1.5.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion/*,
  "org.apache.spark" %% "spark-hive" % hiveVersion*/
)

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.0" % "test"

assemblySettings
jarName in assembly := "my-project-assembly.jar"
assemblyOption in assembly :=
  (assemblyOption in assembly).value.copy(includeScala = false)



    