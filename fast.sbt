name := "fastRuleMining"

version := "1.0"

scalaVersion := "2.11.8"

val sparkVersion = "2.2.0"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion
)

fork in run := true
javaOptions in run ++= Seq(
  "-Dlog4j.debug=false",
  "-Dlog4j.configuration=log4j.properties")
outputStrategy := Some(StdoutOutput)
