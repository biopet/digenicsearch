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

import nl.biopet.utils.ngs.ped.{PedigreeFile, PedigreeSample, Phenotype}

case class PedigreeFileArray(pedFile: PedigreeFile, samples: Array[String]) {
  val pedArray: Array[PedigreeSample] = samples.flatMap(pedFile.samples.get)
  val affectedArray: Array[Int] = pedArray.zipWithIndex
    .filter { case (p, _) => p.phenotype == Phenotype.Affected }
    .map { case (_, idx) => idx }
  val unaffectedArray: Array[Int] = pedArray.zipWithIndex
    .filter { case (p, _) => p.phenotype == Phenotype.Unaffected }
    .map { case (_, idx) => idx }
}

object PedigreeFileArray {
  def fromFiles(files: List[File], samples: Array[String]): PedigreeFileArray = {
    val pedSamples = files.map(PedigreeFile.fromFile).reduce(_ + _)
    PedigreeFileArray(new PedigreeFile(pedSamples.samples.filter {
      case (s, _) =>
        samples.contains(s)
    }), samples)
  }
}
