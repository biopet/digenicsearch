/*
 * Copyright (c) 2017 Sequencing Analysis Support Core - Leiden University Medical Center
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package nl.biopet.tools.digenicsearch

import java.io.File

import nl.biopet.utils.tool.{AbstractOptParser, ToolCommand}

class ArgsParser(toolCommand: ToolCommand[Args])
    extends AbstractOptParser[Args](toolCommand) {

  opt[File]('i', "inputFile")
    .required()
    .action((x, c) => c.copy(inputFiles = x :: c.inputFiles))
    .text("Input vcf files")
  opt[File]('o', "outputDir")
    .required()
    .action((x, c) => c.copy(outputDir = x))
    .text("Output dir for the tool")
  opt[File]('R', "reference")
    .required()
    .action((x, c) => c.copy(reference = x))
    .text("Reference fasta file to use, dict file should be next to it")
  opt[File]("regions")
    .action((x, c) => c.copy(regions = Some(x)))
    .text("Only using this regions in the bed file")
  opt[File]('p', "pedFile")
    .required()
    .unbounded()
    .action((x, c) => c.copy(pedFiles = x :: c.pedFiles))
    .text("Input ped file for family relations and effected/non-effected")
  opt[String]("detectionMode")
    .action(
      (x, c) =>
        c.copy(
          detectionMode = DetectionMode.values
            .find(_.toString.toLowerCase == x.toLowerCase)
            .getOrElse(throw new IllegalArgumentException(
              s"Value '$x' not allowed, possible values: ${DetectionMode.values
                .mkString(", ")}"))))
    .text(
      s"Detection mode, possible values: ${DetectionMode.values.mkString(", ")}")
  opt[String]("singleAnnotationFilter")
    .unbounded()
    .action {
      case (x, c) =>
        c.copy(
          singleAnnotationFilter = c.singleAnnotationFilter :+ ArgsParser
            .parseAnnotationFilter(x))
    }
    .text("Filter on single variant")
  opt[String]("pairAnnotationFilter")
    .unbounded()
    .action {
      case (x, c) =>
        c.copy(
          pairAnnotationFilter = c.pairAnnotationFilter :+ ArgsParser
            .parseAnnotationFilter(x))
    }
    .text("Filter on paired variant, must be true for 1 of the 2 in the pair")
  opt[Double]("singleAffectedFraction")
    .action { (x, c) =>
      c.copy(fractions = c.fractions.copy(singleAffectedFraction = x))
    }
    .text("minimal affected fraction for each variant")
  opt[Double]("pairAffectedFraction")
    .action { (x, c) =>
      c.copy(fractions = c.fractions.copy(pairAffectedFraction = x))
    }
    .text("minimal affected fraction for for at least 1 of the 2 variants")
  opt[Double]("singleUnaffectedFraction")
    .action { (x, c) =>
      c.copy(fractions = c.fractions.copy(singleUnaffectedFraction = x))
    }
    .text("maximum unaffected fraction for for each variant")
  opt[Double]("pairUnaffectedFraction")
    .action { (x, c) =>
      c.copy(fractions = c.fractions.copy(pairUnaffectedFraction = x))
    }
    .text("maximum unaffected fraction for for at least 1 of the 2 variants")
  opt[Long]("maxDistance")
    .action { (x, c) =>
      c.copy(maxDistance = Some(x))
    }
    .text("maxDistance in base pairs. This option will make the assumption that both variants are on the same contig")
  opt[Int]("binSize")
    .action { (x, c) =>
      c.copy(binSize = x)
    }
    .text("Binsize in estimated base pairs")
  opt[Int]("maxContigsInSingleJob")
    .unbounded()
    .action { (x, c) =>
      c.copy(maxContigsInSingleJob = x)
    }
    .text(s"Max number of bins to be combined, default is 250")
  opt[(String, File)]("externalFile")
    .unbounded()
    .action {
      case ((key, value), c) =>
        if (c.externalFiles.contains(key))
          throw new IllegalArgumentException(s"Key '$key' already exist")
        c.copy(externalFiles = c.externalFiles ++ Map(key -> value))
    }
    .text(s"External file used for filtering")
  opt[String]("singleExternalFilter")
    .unbounded()
    .action {
      case (x, c) =>
        c.copy(
          singleExternalFilters = c.singleExternalFilters :+ ArgsParser
            .parseAnnotationFilter(x))
    }
    .text("Filter on paired variant, must be true for 1 of the 2 in the pair")
  opt[String]("pairExternalFilter")
    .unbounded()
    .action {
      case (x, c) =>
        c.copy(
          pairExternalFilters = c.pairExternalFilters :+ ArgsParser
            .parseAnnotationFilter(x))
    }
    .text("Filter on paired variant, must be true for 1 of the 2 in the pair")
  opt[String]("sparkMaster")
    .action((x, c) => c.copy(sparkMaster = Some(x)))
    .text("Spark master, default to local[1]")
}

object ArgsParser {
  def parseAnnotationFilter(arg: String): AnnotationFilter = {
    arg match {
      case a if a.contains(">=") =>
        val split = arg.split(">=", 2)
        AnnotationFilter(split(0), _ >= split(1).toDouble)
      case a if a.contains("<=") =>
        val split = arg.split("<=", 2)
        AnnotationFilter(split(0), _ <= split(1).toDouble)
      case _ =>
        throw new IllegalArgumentException(
          "No method found, possible methods: >=, <=")
    }
  }
}
