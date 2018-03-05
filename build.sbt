organization := "com.github.biopet"
organizationName := "Biopet"

startYear := Some(2017)

name := "DigenicSearch"
biopetUrlName := "digenicsearch"

biopetIsTool := true

developers += Developer(id = "ffinfo",
                        name = "Peter van 't Hof",
                        email = "pjrvanthof@gmail.com",
                        url = url("https://github.com/ffinfo"))

fork in Test := true

scalaVersion := "2.11.12"

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.8.7"

libraryDependencies += "com.github.biopet" %% "common-utils" % "0.3.1"
libraryDependencies += "com.github.biopet" %% "spark-utils" % "0.3.1"
libraryDependencies += "com.github.biopet" %% "tool-utils" % "0.3.1"
libraryDependencies += "com.github.biopet" %% "ngs-utils" % "0.3.1"
libraryDependencies += "com.github.biopet" %% "tool-test-utils" % "0.2.2" % Test

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.1" % Provided
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.1" % Provided

mainClass in assembly := Some("nl.biopet.tools.digenicsearch.DigenicSearch")
