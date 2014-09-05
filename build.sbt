
name := "dynasty"

organization := "com.gravitydev"

version := "0.1.12-SNAPSHOT"

scalaVersion := "2.11.2"

crossScalaVersions := Seq("2.11.2", "2.10.4")

libraryDependencies ++= Seq(
  "com.amazonaws"             % "aws-java-sdk" % "1.8.9.1",
  "com.typesafe.scala-logging" %% "scala-logging-slf4j" % "2.1.2",
  "org.scalatest" % "scalatest_2.10" % "2.0" % "test",
  "joda-time" % "joda-time" % "2.1" % "test",
  "org.joda" % "joda-convert" % "1.1" % "test"
)

offline := true

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
