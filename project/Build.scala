import sbt._
import Keys._

object ApplicationBuild extends Build {

  lazy val dynasty = Project("dynasty", file("."))
    .settings(
      organization := "com.gravitydev",
      version := "0.0.18-SNAPSHOT",
      scalaVersion := "2.11.1",
      crossScalaVersions := Seq("2.11.1", "2.10.4"),
      libraryDependencies ++= Seq(
        "com.amazonaws"             % "aws-java-sdk" % "1.7.1",
        "com.typesafe.scala-logging" %% "scala-logging-slf4j" % "2.1.2",

        // test
        "org.scalatest" % "scalatest_2.10" % "2.0" % "test",
        "joda-time" % "joda-time" % "2.1" % "test",
        "org.joda" % "joda-convert" % "1.1" % "test"
      ),
      offline := true,
      publishTo := Some(gravityRepo),
      publishArtifact in (Compile, packageDoc) := false,
      scalacOptions ++= Seq("-deprecation", "-unchecked", "-Xcheckinit", "-encoding", "utf8", "-feature", "-language:postfixOps", "-Xlint", "-language:implicitConversions")
      //EclipseKeys.relativizeLibs := false,
      //EclipseKeys.withSource := true,
      //resolvers ++= commonResolvers
    )

  val commonResolvers = Seq(
    //gravityRepo//,
    //"gravity" at "http://repos.gravitydev.com/app/repos/12",
    //"gravity2" at "http://repo.gravitydev.com/"
  )

  val gravityRepo = "gravitydev" at "https://devstack.io/repo/gravitydev/public"
}

