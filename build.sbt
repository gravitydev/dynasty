
name := "dynasty"

organization := "com.gravitydev"

version := "0.2.0-SNAPSHOT-underlying-type9"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0",
  "com.gravitydev" %% "awsutil" % "0.0.5-SNAPSHOT",
  "org.scalatest" %% "scalatest" % "2.2.5" % "test",
  "joda-time" % "joda-time" % "2.8" % "test",
  "org.joda" % "joda-convert" % "1.7" % "test",
  "com.typesafe.akka" % "akka-stream_2.11" % "2.4.8",
  "ch.qos.logback" % "logback-classic" % "1.1.7" % "test"

)

offline := true

resolvers += "gravitydev" at "https://devstack.io/repo/gravitydev/public"

parallelExecution in Test := false

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

