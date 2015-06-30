
name := "dynasty"

organization := "com.gravitydev"

version := "0.2.0-SNAPSHOT-underlying-type3"

scalaVersion := "2.11.6"

libraryDependencies ++= Seq(
  "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0",
  "com.gravitydev" %% "awsutil" % "0.0.4-SNAPSHOT",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test",
  "joda-time" % "joda-time" % "2.8" % "test",
  "org.joda" % "joda-convert" % "1.7" % "test"
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

