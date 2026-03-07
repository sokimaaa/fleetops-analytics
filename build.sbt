ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.16"

lazy val sparkVersion = "4.0.1"

lazy val root = (project in file("."))
  .settings(
    name := "FleetOps Analytics",
    organization := "com.fleetops",

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql"  % sparkVersion,
      "com.typesafe"     %  "config"     % "1.4.3",
      "org.scalatest"    %% "scalatest"  % "3.2.19" % Test
    )
  )
