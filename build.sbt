name := "akka-nats-streaming"

version := "0.1"

scalaVersion := "2.12.10"

crossScalaVersions := Seq("2.12.10", "2.13.1")

libraryDependencies ++= Seq(
  "io.nats"           %  "java-nats-streaming" % "2.2.3",
  "com.typesafe.akka" %% "akka-stream"         % "2.6.1",
  "org.scala-lang.modules" %% "scala-collection-compat" % "2.1.3"
)
