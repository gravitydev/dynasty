
name := "dynasty"

organization := "com.gravitydev"

version := "0.2.1-SNAPSHOT"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0",
  "com.amazonaws" % "aws-java-sdk-dynamodb" % "1.11.58",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test",
  "joda-time" % "joda-time" % "2.9.6" % "test",
  "org.joda" % "joda-convert" % "1.8.1" % "test"
)

offline := true

resolvers += "gravitydev" at "https://devstack.io/repo/gravitydev/public"

publishTo := Some("gravitydev" at "https://devstack.io/repo/gravitydev/public")

publishArtifact in (Compile, packageDoc) := false

scalacOptions ++= Seq(
  "-deprecation", 
  "-unchecked", 
  "-Xcheckinit", 
  "-encoding", 
  "utf8", 
  "-feature", 
  "-language:postfixOps", 
  "-Xlint", 
  "-language:implicitConversions"
)

