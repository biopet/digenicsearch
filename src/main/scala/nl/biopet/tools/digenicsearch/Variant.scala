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

case class Variant(contig: String,
                   pos: Int,
                   alleles: List[String],
                   genotypes: List[Genotype],
                   annotations: List[AnnotationValue] = List(),
                   genotypeAnnotation: List[GenotypeAnnotation],
                   detectionResult: DetectionMode.DetectionResult) {

  def filterSingleFraction(pedigree: PedigreeFileArray,
                           cutoffs: FractionsCutoffs): Option[Variant] = {

    val alleles = detectionResult.result.toMap

    val result = for ((allele, result) <- alleles) yield {
      val affectedGenotypes = pedigree.affectedArray.map(result)
      val unaffectedGenotypes = pedigree.unaffectedArray.map(result)

      val unaffectedFraction =
        if (unaffectedGenotypes.nonEmpty) {
          unaffectedGenotypes
            .count(_ == true)
            .toDouble / unaffectedGenotypes.length
        } else 0.0
      val affectedFraction = affectedGenotypes
        .count(_ == true)
        .toDouble / affectedGenotypes.length

      allele -> PedigreeFraction(affectedFraction, unaffectedFraction)
    }

    val filter = result
      .filter(_._2.unaffected <= cutoffs.singleUnaffectedFraction)
      .filter(_._2.affected >= cutoffs.singleAffectedFraction)

    if (filter.nonEmpty) {
      Some(
        this.copy(detectionResult =
          DetectionResult(filter.keys.map(k => k -> alleles(k)).toList)))
    } else None
  }
}

object Variant {
  def filterPairFraction(
      v1: Variant,
      v2: Variant,
      pedigree: PedigreeFileArray,
      cutoffs: FractionsCutoffs): Option[(Variant, Variant)] = {
    val alleles1 = v1.detectionResult.result.toMap
    val alleles2 = v2.detectionResult.result.toMap

    val alleles = (for ((a1, s1) <- alleles1; (a2, s2) <- alleles2) yield {
      val combined = s1.zip(s2).map { case (c1, c2) => c1 && c2 }
      val affectedGenotypes = pedigree.affectedArray.map(combined)
      val unaffectedGenotypes = pedigree.unaffectedArray.map(combined)

      val unaffectedFraction =
        if (unaffectedGenotypes.nonEmpty) {
          unaffectedGenotypes
            .count(_ == true)
            .toDouble / unaffectedGenotypes.length
        } else 0.0
      val affectedFraction = affectedGenotypes
        .count(_ == true)
        .toDouble / affectedGenotypes.length

      if (unaffectedFraction <= cutoffs.pairUnaffectedFraction && affectedFraction >= cutoffs.pairAffectedFraction) {
        Option((a1, a2))
      } else None
    }).flatten

    if (alleles.nonEmpty) {
      val a1 = alleles.map(_._1).toList.toSet
      val a2 = alleles.map(_._2).toList.toSet
      val d1 = DetectionResult(v1.detectionResult.result.filter {
        case (a, _) => a1.contains(a)
      })
      val d2 = DetectionResult(v2.detectionResult.result.filter {
        case (a, _) => a2.contains(a)
      })
      Option((v1.copy(detectionResult = d1), v2.copy(detectionResult = d2)))
    } else None
  }
}
