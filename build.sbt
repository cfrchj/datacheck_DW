import AssemblyKeys._

assemblySettings

name := "datacheck_DW"

version := "0.0.8withfold"

scalaVersion := "2.10.6"

resolvers += "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.6.0" %  "provided"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "1.6.0" %  "provided"

libraryDependencies += "org.apache.hbase" % "hbase-client" % "1.0.0-cdh5.4.2"  %  "provided"

libraryDependencies += "org.apache.hbase" % "hbase-common" % "1.0.0-cdh5.4.2"  %  "provided"

libraryDependencies += "org.apache.hbase" % "hbase-server" % "1.0.0-cdh5.4.2"  %  "provided"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.0" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-hive" % "1.6.0" % "provided"

libraryDependencies += "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.6.0" % "provided"

libraryDependencies += "org.apache.kafka" % "kafka-clients" % "0.10.0.0" % "provided"

libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.6"

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
