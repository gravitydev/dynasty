import sbt._
import Keys._
//import com.typesafe.sbteclipse.plugin.EclipsePlugin.EclipseKeys

object ApplicationBuild extends Build {

  lazy val dynasty = Project("dynasty", file("."))
    .settings(
      organization := "com.gravitydev",
      version := "0.0.9-SNAPSHOT",
      scalaVersion := "2.10.0",
      libraryDependencies ++= Seq(
        "com.amazonaws"             % "aws-java-sdk" % "1.5.8",
        "org.slf4j"                 % "slf4j-api"    % "1.7.5",

        // test
        "org.scalatest" %%  "scalatest"             % "1.8"     % "test" cross CrossVersion.full,
        "joda-time" % "joda-time" % "2.1" % "test",
        "org.joda" % "joda-convert" % "1.1" % "test"
      ),
      offline := true,
      publishTo := Some(gravityRepo),
      publishArtifact in (Compile, packageDoc) := false,
      scalacOptions ++= Seq("-deprecation", "-unchecked", "-Xcheckinit", "-encoding", "utf8", "-feature", "-language:postfixOps", "-Xlint", "-language:implicitConversions"),
      //EclipseKeys.relativizeLibs := false,
      //EclipseKeys.withSource := true,
      resolvers ++= commonResolvers
    )

  val commonResolvers = Seq(
    "gravity" at "http://repos.gravitydev.com/app/repos/12",
    "gravity2" at "http://repo.gravitydev.com/"
  )

  val gravityRepo = "gravitydev" at "http://repos.gravitydev.com/app/repos/12"
}

