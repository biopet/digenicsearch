/*
 * Copyright (c) 2017 Biopet
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

import nl.biopet.utils.tool.ToolCommand

object DigenicSearch extends ToolCommand[Args] {
  def emptyArgs = Args()
  def argsParser = new ArgsParser(this)

  def main(args: Array[String]): Unit = {
    val cmdArgs = cmdArrayToArgs(args)
    SparkMethods.main(cmdArgs)
  }

  def descriptionText: String =
    """
      |This will will search for a combination of variants within a multi sample vcf file.
      |The tool can filter on INFO fields and a maximum distance of the snps on the reference.
    """.stripMargin

  def manualText: String =
    """
      |Because of the number of possible combination this tool requires to run on a spark cluster.
      |If required the tool can still run local by submitting the tool to a local master, see also https://spark.apache.org/docs/latest/submitting-applications.html#master-urls
      |By default this tool runs on the complete genome but with the option --regions a bed file can be provided to limit the number of locations
    """.stripMargin

  def exampleText: String =
    s"""
      |A default run:
      |${example("-i",
                 "<input vcf>",
                 "-o",
                 "<output dir>",
                 "-R",
                 "<reference fasta>",
                 "-p",
                 "<ped file")}
      |
      |A run on limited locations:
      |${example("-i",
                 "<input vcf>",
                 "-o",
                 "<output dir>",
                 "-R",
                 "<reference fasta>",
                 "--regions",
                 "<bed file>",
                 "-p",
                 "<ped file")}
    """.stripMargin
}
