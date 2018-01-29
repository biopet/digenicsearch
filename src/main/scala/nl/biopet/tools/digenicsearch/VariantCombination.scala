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

import nl.biopet.tools.digenicsearch.DetectionMode.DetectionResult

case class VariantCombination(v1: Variant,
                              v2: Variant,
                              alleles: List[AlleleCombination]) {
  def cleanResults: VariantCombination = {
    cleanResults(alleles)
  }

  def cleanResults(keep: List[AlleleCombination]): VariantCombination = {
    val alleles1 = keep.map(_.a1).toSet
    val alleles2 = keep.map(_.a2).toSet
    val detectionResult1 = DetectionResult(v1.detectionResult.result.filter {
      case (a, _) => alleles1.contains(a)
    })
    val externalDetectionResult1 = v1.externalDetectionResult.map(x =>
      DetectionResult(x.result.filter { case (a, _) => alleles1.contains(a) }))
    val newV1 = v1.copy(
      detectionResult = detectionResult1,
      externalDetectionResult = externalDetectionResult1
    )
    val detectionResult2 = DetectionResult(v2.detectionResult.result.filter {
      case (a, _) => alleles2.contains(a)
    })
    val externalDetectionResult2 = v2.externalDetectionResult.map(x =>
      DetectionResult(x.result.filter { case (a, _) => alleles2.contains(a) }))
    val newV2 = v2.copy(
      detectionResult = detectionResult2,
      externalDetectionResult = externalDetectionResult2
    )
    this.copy(v1 = newV1, v2 = newV2, alleles = keep)
  }

  def filterPairFraction(
      pedigree: PedigreeFileArray,
      cutoffs: FractionsCutoffs): Option[VariantCombination] = {

    val alleles1 = v1.detectionResult.result.toMap
    val alleles2 = v2.detectionResult.result.toMap

    val newAlleles = alleles.filter { alleles =>
      val combined = alleles1(alleles.a1).zip(alleles2(alleles.a2)).map {
        case (c1, c2) => c1 && c2
      }
      val affectedGenotypes = pedigree.affectedArray.map(combined)
      val unaffectedGenotypes = pedigree.unaffectedArray.map(combined)

      Variant.unaffectedFraction(unaffectedGenotypes) <= cutoffs.pairUnaffectedFraction &&
      Variant.affectedFraction(affectedGenotypes) >= cutoffs.pairAffectedFraction
    }

    if (newAlleles.nonEmpty) Some(this.copy(alleles = newAlleles))
    else None
  }

  def filterExternalPair(broadcasts: Broadcasts): Option[VariantCombination] = {

    val newAlleles = alleles.filter { alleles =>
      v1.externalDetectionResult
        .zip(v2.externalDetectionResult)
        .zipWithIndex
        .forall {
          case ((a1, a2), idx) =>
            val alleles1 = a1.result.toMap
            val alleles2 = a2.result.toMap

            val allSamples = alleles1(alleles.a1).zip(alleles2(alleles.a2))
            val fraction = allSamples.count { case (a, b) => a && b }.toDouble / allSamples.length
            broadcasts
              .pairExternalFilters(idx)
              .forall(filter => filter.method(fraction))
        }
    }

    if (newAlleles.nonEmpty) Some(this.copy(alleles = newAlleles))
    else None
  }

  def toResultLine: ResultLine =
    ResultLine(v1.contig, v1.pos, v2.contig, v2.pos)
}
