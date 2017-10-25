name := "rdb-connector-common"

version := "0.1-SNAPSHOT"

scalaVersion := "2.12.4"


libraryDependencies ++= {
  val scalaTestV = "3.0.1"
  Seq(
    "org.scalatest" %% "scalatest" % scalaTestV % "test"
  )
}