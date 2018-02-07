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

/** This stores a variant and genotypes in a compressed format */
case class Variant(
    contig: Int,
    pos: Int,
    alleles: List[String],
    genotypes: List[Genotype],
    annotations: List[AnnotationValue] = List(),
    genotypeAnnotation: List[GenotypeAnnotation],
    detectionResult: DetectionMode.DetectionResult,
    externalGenotypes: Array[List[Genotype]],
    externalDetectionResult: Array[DetectionMode.DetectionResult],
    regionsIdx: Int) {

  /** Convert this to a csv object */
  def toCsv(broadcasts: Broadcasts): VariantCsv = {
    val pedigreeFractions = getAffectedFractions(broadcasts)
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
    VariantCsv(broadcasts.dict.getSequence(contig).getSequenceName,
               pos,
               alleles.mkString(","),
               pedigreeFractions,
               externalFractions)
  }

  /** Get fractions for the complete input set */
  def getAffectedFractions(
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

  /** Returns all fractions for each family */
  def getFamilyFractions(
      broadcasts: Broadcasts): Array[Map[List[Short], PedigreeFraction]] = {
    val alleles = detectionResult.result.toMap

    for (family <- broadcasts.pedigree.families.indices.toArray) yield {
      for ((allele, result) <- alleles) yield {
        val affectedGenotypes =
          broadcasts.pedigree.familiesAffected(family).map(result)
        val unaffectedGenotypes =
          broadcasts.pedigree.familiesUnaffected(family).map(result)

        allele -> PedigreeFraction(
          Variant.affectedFraction(affectedGenotypes),
          Variant.unaffectedFraction(unaffectedGenotypes))
      }
    }
  }

  /** Filter based on family fractions, usingOtherFamilies argument is used here */
  def filterFamilyFractions(broadcasts: Broadcasts): Option[Variant] = {
    val fractions = getFamilyFractions(broadcasts)

    val alleles = detectionResult.result.map { case (a, _) => a }

    val teRemove = alleles.filter { allele =>
      // creates a array of families which families are passed
      val keep = fractions.map { family =>
        val f = family(allele)
        (f.unaffected <= broadcasts.fractionsCutoffs.singleFamilyUnaffectedFraction) && (f.affected >= broadcasts.fractionsCutoffs.singleFamilyAffectedFraction)
      }

      if (keep.forall(_ == true)) true
      else {
        val remove = keep.forall(_ == false)
        if (!keep.forall(_ == false) && broadcasts.usingOtherFamilies) {
          keep.zipWithIndex
            .flatMap { case (b, idx) => if (b) None else Some(idx) }
            .forall(fractions(_)(allele).affected <= 0.0)
        } else remove
      }
    }

    removeAlleles(teRemove.toSet)
  }

  /** Returns a array with all families, true if they pass the fractions filters */
  def passedFamilies(broadcasts: Broadcasts): Array[Boolean] = {
    val alleles = detectionResult.result.map { case (a, _) => a }

    getFamilyFractions(broadcasts).map { map =>
      alleles.exists { allele =>
        val f = map(allele)
        !(f.unaffected <= broadcasts.fractionsCutoffs.singleFamilyUnaffectedFraction) || !(f.affected >= broadcasts.fractionsCutoffs.singleFamilyAffectedFraction)
      }
    }
  }

  /** This will remove filter variants on affected / unaffected fractions set */
  def filterSingleFraction(broadcasts: Broadcasts): Option[Variant] = {

    val result = this.getAffectedFractions(broadcasts)

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

  /** This will remove filter variants on external fractions set on the commandline */
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

  /** This method will remove alleles and create a new Variant object */
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

  /** This calculates the unaffected fraction */
  def unaffectedFraction(unaffectedGenotypes: Array[Boolean]): Double = {
    if (unaffectedGenotypes.nonEmpty) {
      unaffectedGenotypes
        .count(_ == true)
        .toDouble / unaffectedGenotypes.length
    } else 0.0
  }

  /** This calculates the affected fraction */
  def affectedFraction(affectedGenotypes: Array[Boolean]): Double = {
    affectedGenotypes
      .count(_ == true)
      .toDouble / affectedGenotypes.length
  }

  /** This returns all possible allele combinations */
  def alleleCombinations(v1: Variant,
                         v2: Variant): Iterator[AlleleCombination] = {
    v1.detectionResult.result.map { case (allele, _) => allele }
    for {
      (a1, _) <- v1.detectionResult.result.iterator
      (a2, _) <- v2.detectionResult.result
    } yield AlleleCombination(a1, a2)
  }
}
