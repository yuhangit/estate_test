name := "EstateTest"

version := "1.0"

scalaVersion := "2.11.8"

sbtVersion := "1.0.4"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.2.0" % Provided ,
  "org.apache.spark" %% "spark-sql" % "2.2.0" % Provided
)

// https://mvnrepository.com/artifact/org.apache.commons/commons-io
libraryDependencies += "org.apache.commons" % "commons-io" % "1.3.2"


unmanagedBase := baseDirectory.value / "lib"