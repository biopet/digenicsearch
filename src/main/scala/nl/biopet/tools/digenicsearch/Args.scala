package nl.biopet.tools.digenicsearch

import java.io.File

case class Args(inputFiles: List[File] = Nil,
                outputDir: File = null,
                reference: File = null,
                regions: Option[File] = None,
                singleAnnotationFilter: List[(String, Double => Boolean)] = Nil,
                pairAnnotationFilter: List[(String, Double => Boolean)] = Nil,
                binSize: Int = 10000000,
                maxContigsInSingleJob: Int = 250,
                sparkMaster: Option[String] = None,
                sparkConfigValues: Map[String, String] = Map(
                  "spark.driver.maxResultSize" -> "2G",
                  "spark.driver.memory" -> "2G"
                ))
