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

import nl.biopet.tools.digenicsearch.DetectionMode.DetectionResult

case class VariantCombination(v1: Variant,
                              v2: Variant,
                              alleles: List[AlleleCombination]) {
  def cleanResults: VariantCombination = {
    cleanResults(alleles)
  }

  /** This method will remove alleles that are not used anymore in detection result because of filtering */
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

  /** Returns fractions for each allele combination */
  def getFractions(
      broadcasts: Broadcasts,
      familyId: Option[Int] = None): Map[AlleleCombination, Fraction] = {
    val alleles1 = v1.detectionResult.result.toMap
    val alleles2 = v2.detectionResult.result.toMap

    alleles.map { c =>
      val combined = alleles1(c.a1).zip(alleles2(c.a2)).map {
        case (c1, c2) => c1 && c2
      }
      val affectedGenotypes = familyId match {
        case Some(id) => broadcasts.pedigree.familiesAffected(id).map(combined)
        case _        => broadcasts.pedigree.affectedArray.map(combined)
      }
      val unaffectedGenotypes = familyId match {
        case Some(id) =>
          broadcasts.pedigree.familiesUnaffected(id).map(combined)
        case _ => broadcasts.pedigree.unaffectedArray.map(combined)
      }
      c -> Fraction(Variant.affectedFraction(affectedGenotypes),
                    Variant.unaffectedFraction(unaffectedGenotypes))
    }.toMap
  }

  /** Filter based on fractions */
  def filterPairFraction(
      broadcasts: Broadcasts,
      familyId: Option[Int] = None): Option[VariantCombination] = {

    val fractions = getFractions(broadcasts, familyId)

    val newAlleles = alleles.filter { alleles =>
      fractions(alleles).unaffected <= broadcasts.fractionsCutoffs.pairUnaffectedFraction &&
      fractions(alleles).affected >= broadcasts.fractionsCutoffs.pairAffectedFraction
    }

    if (newAlleles.nonEmpty) Some(this.copy(alleles = newAlleles))
    else None
  }

  /** returns external fractions of they exist for each combination */
  def externalFractions(idx: Int): Map[AlleleCombination, Option[Double]] = {
    val alleles1 = this.v1.externalDetectionResult(idx).result.toMap
    val alleles2 = this.v2.externalDetectionResult(idx).result.toMap

    (for (c <- alleles.toIterator
          if alleles1.contains(c.a1) && alleles2.contains(c.a2)) yield {
      val allSamples = alleles1(c.a1).zip(alleles2(c.a2))
      val fraction = allSamples.count {
        case (a, b) =>
          a && b
      }.toDouble / allSamples.length
      if (fraction.isNaN) (c, None)
      else (c, Some(fraction))
    }).toMap
  }

  /** Filter based on external fractions */
  def filterExternalPair(broadcasts: Broadcasts): Option[VariantCombination] = {

    broadcasts.externalFiles.zipWithIndex.foldLeft(Option(this)) {
      case (c, (_, idx)) =>
        c.flatMap { x =>
          val fractions = externalFractions(idx)
          val newAlleles = alleles.filter { a =>
            broadcasts
              .pairExternalFilters(idx)
              .forall { filter =>
                fractions.get(a).flatten match {
                  case Some(s) => filter.method(s)
                  case _       => true
                }
              }
          }
          if (newAlleles.nonEmpty) Some(this.copy(alleles = newAlleles))
          else None
        }
    }
  }

  /** Convert to a result line */
  def toResultLine(broadcasts: Broadcasts): ResultLineCsv = {
    val fractions = getFractions(broadcasts)
      .map {
        case (c, f) =>
          s"$c:a=${f.affected},u=${f.unaffected}"
      }
      .mkString(";")
    val familyFractions = broadcasts.pedigree.families.zipWithIndex
      .map {
        case (name, idx) =>
          getFractions(broadcasts, Some(idx))
            .map {
              case (c, f) =>
                s"$name=($c:a=${f.affected},u=${f.unaffected})"
            }
            .mkString(";")
      }
      .mkString(";")
    val externalFractions: String = v1.externalDetectionResult.indices
      .map(
        idx =>
          broadcasts.externalFilesKeys(idx) + ":" + this
            .externalFractions(idx)
            .zipWithIndex
            .map { case ((c, f), _) => c.toString + s"=${f.getOrElse(0.0)}" }
            .mkString("(", ";", ")"))
      .mkString(";")
    ResultLineCsv(
      broadcasts.dict.getSequence(v1.contig).getSequenceName,
      v1.pos,
      broadcasts.dict.getSequence(v2.contig).getSequenceName,
      v2.pos,
      fractions,
      familyFractions,
      externalFractions
    )
  }
}
