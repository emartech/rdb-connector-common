name := "rdb-connector-common"

version := "0.1-SNAPSHOT"

scalaVersion := "2.12.3"


libraryDependencies ++= {
  val scalaTestV = "3.0.1"
  Seq(
    "com.typesafe.akka" %% "akka-stream" % "2.5.6",
    "com.typesafe.akka" %% "akka-stream-testkit" % "2.5.6" % Test,
    "org.scalatest" %% "scalatest" % scalaTestV % Test,
    "org.mockito"         %  "mockito-core"         % "2.11.0"  % Test,
    "com.typesafe.akka"     %% "akka-http-spray-json"      % "10.0.7" % Test
  )
}