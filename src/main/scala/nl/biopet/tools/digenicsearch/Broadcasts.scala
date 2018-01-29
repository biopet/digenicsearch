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

import nl.biopet.tools.digenicsearch.DigenicSearch.generateRegions
import nl.biopet.utils.ngs.vcf

case class Broadcasts(samples: Array[String],
                      pedigree: PedigreeFileArray,
                      annotations: Set[String],
                      maxDistance: Option[Long],
                      singleFilters: List[AnnotationFilter],
                      pairFilters: List[AnnotationFilter],
                      inputFiles: List[File],
                      fractionsCutoffs: FractionsCutoffs,
                      detectionMode: DetectionMode.Value,
                      regions: Array[List[Region]])

object Broadcasts {
  def fromArgs(cmdArgs: Args): Broadcasts = {
    val samples: Array[String] =
      cmdArgs.inputFiles.flatMap(vcf.getSampleIds).toArray
    require(samples.lengthCompare(samples.distinct.length) == 0,
            "Duplicated samples detected")

    val pedigree = PedigreeFileArray.fromFiles(cmdArgs.pedFiles, samples)

    samples.foreach(
      id =>
        require(pedigree.pedFile.samples.contains(id),
                s"Sample '$id' not found in ped files"))

    val annotations: Set[String] = cmdArgs.singleAnnotationFilter
      .map(_.key)
      .toSet ++ cmdArgs.pairAnnotationFilter.map(_.key).toSet

    Broadcasts(
      samples,
      pedigree,
      annotations,
      cmdArgs.maxDistance,
      cmdArgs.singleAnnotationFilter,
      cmdArgs.pairAnnotationFilter,
      cmdArgs.inputFiles,
      cmdArgs.fractions,
      cmdArgs.detectionMode,
      generateRegions(cmdArgs).toArray
    )
  }
}
