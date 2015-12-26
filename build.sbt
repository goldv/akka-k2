name := "akka-k2"

version       := "1.0-SNAPSHOT"

scalaVersion  := "2.11.7"

resolvers += "scalaz-bintray" at "https://dl.bintray.com/scalaz/releases"

libraryDependencies ++= Seq(
    "com.typesafe.akka" %% "akka-actor" % "2.4.0",
    "com.typesafe.akka" %% "akka-testkit" % "2.4.0",
    "org.scalatest" % "scalatest_2.11" % "2.2.4" % "test"
)


