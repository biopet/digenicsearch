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

case class FractionsCutoffs(singleAffectedFraction: Double = 1.0,
                            pairAffectedFraction: Double = 1.0,
                            singleUnaffectedFraction: Double = 0,
                            pairUnaffectedFraction: Double = 0) {

  def singleFractionFilter(variant: Variant,
                           pedigree: PedigreeFileArray): Boolean = {
    (variant.affectedFraction, variant.unaffectedFraction) match {
      case (Some(a), Some(u)) =>
        a >= singleAffectedFraction && u <= singleUnaffectedFraction
      case _ =>
        throw new IllegalStateException(
          "Fractions should be known at this point")
    }
  }

  def pairFractionFilter(v1: Variant,
                         v2: Variant,
                         pedigree: PedigreeFileArray): Boolean = {

    (v1.affectedFraction,
     v1.unaffectedFraction,
     v2.affectedFraction,
     v2.unaffectedFraction) match {
      case (Some(a1), Some(u1), Some(a2), Some(u2)) =>
        (a1 >= pairAffectedFraction && u1 <= pairUnaffectedFraction) ||
          (a2 >= pairAffectedFraction && u2 <= pairUnaffectedFraction)
      case _ =>
        throw new IllegalStateException(
          "Fractions should be known at this point")
    }
  }
}

object FractionsCutoffs {

  protected case class Result(unaffected: Double, affected: Double)

  def getFraction(variant: Variant, pedigree: PedigreeFileArray): Variant = {
    val affectedGenotypes = pedigree.affectedArray.map(variant.genotypes)
    val unaffectedGenotypes = pedigree.unaffectedArray.map(variant.genotypes)

    val unaffectedFraction =
      if (unaffectedGenotypes.nonEmpty)
        unaffectedGenotypes
          .count(!_.isReference)
          .toDouble / unaffectedGenotypes.length
      else 0.0
    val affectedFraction = affectedGenotypes
      .count(!_.isReference)
      .toDouble / affectedGenotypes.length

    variant.copy(unaffectedFraction = Some(unaffectedFraction),
                 affectedFraction = Some(affectedFraction))
  }

}
