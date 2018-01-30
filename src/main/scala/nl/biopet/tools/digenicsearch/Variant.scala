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

case class Variant(
    contig: String,
    pos: Int,
    alleles: List[String],
    genotypes: List[Genotype],
    annotations: List[AnnotationValue] = List(),
    genotypeAnnotation: List[GenotypeAnnotation],
    detectionResult: DetectionMode.DetectionResult,
    externalGenotypes: Array[List[Genotype]],
    externalDetectionResult: Array[DetectionMode.DetectionResult]) {

  def toCsv(broadcasts: Broadcasts): VariantCsv = {
    val pedigreeFractions = getPedigreeFractions(broadcasts)
      .map {
        case (k, v) =>
          val allele = if (k.isEmpty) "v" else k.mkString("/")
          s"$allele=(a=${v.affected};u=${v.unaffected})"
      }
      .mkString(",")
    val externalFractions = getExternalFractions.zipWithIndex
      .map {
        case (map, idx) =>
          broadcasts.externalFilesKeys(idx) + map
            .map {
              case (k, v) =>
                (if (k.isEmpty) "v"
                 else k.mkString("/")) + "=" + v.getOrElse("0.0")
            }
            .mkString("(", ",", ")")
      }
      .mkString(";")
    VariantCsv(contig,
               pos,
               alleles.mkString(","),
               pedigreeFractions,
               externalFractions)
  }

  def getPedigreeFractions(
      broadcasts: Broadcasts): Map[List[Short], PedigreeFraction] = {
    val alleles = detectionResult.result.toMap

    for ((allele, result) <- alleles) yield {
      val affectedGenotypes = broadcasts.pedigree.affectedArray.map(result)
      val unaffectedGenotypes = broadcasts.pedigree.unaffectedArray.map(result)

      allele -> PedigreeFraction(
        Variant.affectedFraction(affectedGenotypes),
        Variant.unaffectedFraction(unaffectedGenotypes))
    }
  }

  def filterSingleFraction(broadcasts: Broadcasts): Option[Variant] = {

    val alleles = detectionResult.result.toMap

    val result = this.getPedigreeFractions(broadcasts)

    val teRemove = result
      .filter {
        case (_, f) =>
          !(f.unaffected <= broadcasts.fractionsCutoffs.singleUnaffectedFraction) || !(f.affected >= broadcasts.fractionsCutoffs.singleAffectedFraction)
      }
      .map { case (a, _) => a }
      .toSet

    removeAlleles(teRemove)
  }

  def getExternalFractions: Array[Map[List[Short], Option[Double]]] = {
    externalDetectionResult.map { d =>
      d.result.toMap.map {
        case (k, v) =>
          if (v.isEmpty) k -> None
          else k -> Some(v.count(_ == true).toDouble / v.length)
      }
    }
  }

  def filterExternalFractions(broadcasts: Broadcasts): Option[Variant] = {

    val fractions = getExternalFractions

    val toRemove = (for {
      (dr, filters) <- fractions.zip(broadcasts.singleExternalFilters)
      (allele, fraction) <- dr
    } yield {
      fraction match {
        case Some(f) =>
          if (filters.forall { filter =>
                filter.method(f)
              }) None
          else Some(allele)
        case _ => None
      }
    }).flatten.toSet
    removeAlleles(toRemove)
  }

  private def removeAlleles(removeAlleles: Set[List[Short]]): Option[Variant] = {
    val dr = DetectionResult(this.detectionResult.result.filter {
      case (allele, _) => !removeAlleles.contains(allele)
    })
    val edr = externalDetectionResult.map(x =>
      DetectionResult(x.result.filter {
        case (allele, _) => !removeAlleles.contains(allele)
      }))
    if (dr.result.nonEmpty)
      Some(this.copy(detectionResult = dr, externalDetectionResult = edr))
    else None
  }
}

object Variant {

  def unaffectedFraction(unaffectedGenotypes: Array[Boolean]): Double = {
    if (unaffectedGenotypes.nonEmpty) {
      unaffectedGenotypes
        .count(_ == true)
        .toDouble / unaffectedGenotypes.length
    } else 0.0
  }

  def affectedFraction(affectedGenotypes: Array[Boolean]): Double = {
    affectedGenotypes
      .count(_ == true)
      .toDouble / affectedGenotypes.length
  }

  def alleleCombinations(v1: Variant,
                         v2: Variant): Iterator[AlleleCombination] = {
    v1.detectionResult.result.map { case (allele, _) => allele }
    for {
      (a1, _) <- v1.detectionResult.result.iterator
      (a2, _) <- v2.detectionResult.result
    } yield AlleleCombination(a1, a2)
  }
}
