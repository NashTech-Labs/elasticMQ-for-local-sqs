name := "elasticMQ-sqs-scala-template"

version := "0.1"

scalaVersion := "2.13.6"

// https://mvnrepository.com/artifact/software.amazon.awssdk/sqs
libraryDependencies += "software.amazon.awssdk" % "sqs" % "2.17.12"
// https://mvnrepository.com/artifact/com.typesafe.akka/akka-actor
libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.6.15"
// https://mvnrepository.com/artifact/org.elasticmq/elasticmq-server
libraryDependencies += "org.elasticmq" %% "elasticmq-server" % "1.2.1"

