import sbt._
import Keys._
//import com.typesafe.sbteclipse.plugin.EclipsePlugin.EclipseKeys

object ApplicationBuild extends Build {

  lazy val dynasty = Project("dynasty", file("."))
    .settings(
      organization := "com.gravitydev",
      version := "0.0.6-SNAPSHOT",
      scalaVersion := "2.10.0",
      libraryDependencies ++= Seq(
        "com.typesafe.akka"         %% "akka-actor"   % "2.1.0",
        "com.amazonaws"             % "aws-java-sdk" % "1.5.6",
        "org.slf4j"                 % "slf4j-api"    % "1.7.2"
      ),
      offline := true,
      publishTo := Some(gravityRepo),
      publishArtifact in (Compile, packageDoc) := false,
      scalacOptions ++= Seq("-deprecation", "-unchecked", "-Xcheckinit", "-encoding", "utf8", "-feature", "-language:postfixOps",
        "-language:implicitConversions"),
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

