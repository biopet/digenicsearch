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

  def parseAnnotationFilter(arg: String): (String, Double => Boolean) = {
    if (arg.contains(">=")) {
      val split = arg.split(">=", 2)
      (split(0), _ >= split(1).toDouble)
    } else if (arg.contains("<=")) {
      val split = arg.split("<=", 2)
      (split(0), _ <= split(1).toDouble)
    } else
      throw new IllegalArgumentException(
        "No method found, possible methods: >=, <=")
  }

  opt[File]("inputFile")
    .abbr("i")
    .unbounded()
    .required()
    .action((x, c) => c.copy(inputFiles = x :: c.inputFiles))
    .text("Input vcf files")
  opt[File]('o', "outputDir")
    .required()
    .action((x, c) => c.copy(outputDir = x))
    .text("Outputdir for the tool")
  opt[File]('R', "reference")
    .required()
    .action((x, c) => c.copy(reference = x))
    .text("Reference fasta file to use, dict file should be next to it")
  opt[File]("regions")
    .action((x, c) => c.copy(regions = Some(x)))
    .text("Only using this regions in the bed file")
  opt[File]('p', "pedFile")
    .action((x, c) => c.copy(pedFile = Some(x)))
    .text("Input ped file for family relations and effected/non-effected")
  opt[String]("singleAnnotationFilter")
    .action {
      case (x, c) =>
        c.copy(
          singleAnnotationFilter = c.singleAnnotationFilter :+ parseAnnotationFilter(
            x))
    }
    .text("Filter on single variant")
  opt[String]("pairAnnotationFilter")
    .action {
      case (x, c) =>
        c.copy(
          pairAnnotationFilter = c.pairAnnotationFilter :+ parseAnnotationFilter(
            x))
    }
    .text("Filter on paired variant, must be true for 1 of the 2 in the pair")
  opt[Int]("binSize") action { (x, c) =>
    c.copy(binSize = x)
  } text "Binsize in estimated base pairs"
  opt[Int]("maxContigsInSingleJob") action { (x, c) =>
    c.copy(maxContigsInSingleJob = x)
  } text s"Max number of bins to be combined, default is 250"
  opt[String]("sparkMaster")
    .action((x, c) => c.copy(sparkMaster = Some(x)))
    .text("Spark master, default to local[1]")
  opt[(String, String)]("sparkConfigValue")
    .unbounded()
    .action((x, c) =>
      c.copy(sparkConfigValues = c.sparkConfigValues + (x._1 -> x._2)))
    .text(s"Add values to the spark config")
}
