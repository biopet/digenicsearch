package nl.biopet.tools.digenicsearch

import java.io.File

case class Args(inputFiles: List[File] = Nil,
                outputDir: File = null,
                reference: File = null,
                regions: Option[File] = None,
                binSize: Int = 10000000,
                maxContigsInSingleJob: Int = 250,
                sparkMaster: Option[String] = None,
                sparkConfigValues: Map[String, String] = Map(
                  "spark.driver.maxResultSize" -> "2G",
                  "spark.driver.memory" -> "2G"
                ))
