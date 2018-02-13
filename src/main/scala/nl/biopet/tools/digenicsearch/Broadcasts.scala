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

import java.io.File

import htsjdk.samtools.SAMSequenceDictionary
import nl.biopet.tools.digenicsearch.DigenicSearch.generateRegions
import nl.biopet.utils.ngs.{vcf, fasta}

case class Broadcasts(samples: Array[String],
                      dict: SAMSequenceDictionary,
                      pedigree: PedigreeFileArray,
                      annotations: Set[String],
                      maxDistance: Option[Long],
                      singleFilters: List[AnnotationFilter],
                      pairFilters: List[AnnotationFilter],
                      inputFiles: List[File],
                      fractionsCutoffs: FractionsCutoffs,
                      detectionMode: DetectionMode.Value,
                      regions: Array[List[Region]],
                      externalFiles: Array[File],
                      externalFilesKeys: Array[String],
                      singleExternalFilters: Array[List[ExternalFilter]],
                      pairExternalFilters: Array[List[ExternalFilter]],
                      usingOtherFamilies: Boolean)

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

    (cmdArgs.singleExternalFilters.map(_.key) ++ cmdArgs.pairExternalFilters
      .map(_.key))
      .foreach(
        k =>
          require(cmdArgs.externalFiles.contains(k),
                  s"External file with key '$k' not found"))

    val externalFilesKeys = cmdArgs.externalFiles.toArray
    val externalFiles = externalFilesKeys.map { case (key, file) => file }
    require(externalFilesKeys.length == externalFiles.length,
            "Duplicate external file found")

    val singleExternalFilters = externalFilesKeys.zipWithIndex.map {
      case ((key, file), idx) =>
        cmdArgs.singleExternalFilters
          .filter(_.key == key)
          .map(f => ExternalFilter(key, idx, f.method))
    }
    val pairExternalFilters = externalFilesKeys.zipWithIndex.map {
      case ((key, file), idx) =>
        cmdArgs.pairExternalFilters
          .filter(_.key == key)
          .map(f => ExternalFilter(key, idx, f.method))
    }
    singleExternalFilters.zip(pairExternalFilters).zipWithIndex.foreach {
      case ((single, pair), idx) =>
        require(single.nonEmpty || pair.nonEmpty,
                s"External file is not used in a filter: ${externalFiles(idx)}")
    }

    val dict = fasta.getCachedDict(cmdArgs.reference)

    Broadcasts(
      samples,
      dict,
      pedigree,
      annotations,
      cmdArgs.maxDistance,
      cmdArgs.singleAnnotationFilter,
      cmdArgs.pairAnnotationFilter,
      cmdArgs.inputFiles,
      cmdArgs.fractions,
      cmdArgs.detectionMode,
      generateRegions(cmdArgs, dict).toArray,
      externalFiles,
      externalFilesKeys.map { case (key, _) => key },
      singleExternalFilters,
      pairExternalFilters,
      cmdArgs.usingOtherFamilies
    )
  }
}
