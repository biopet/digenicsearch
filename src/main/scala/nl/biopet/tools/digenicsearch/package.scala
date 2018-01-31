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

package nl.biopet.tools

package object digenicsearch {

  case class AnnotationFilter(key: String, method: Double => Boolean)
  case class ExternalFilter(key: String, id: Int, method: Double => Boolean)

  case class AnnotationValue(key: String, value: List[Double])

  case class Combination(i1: Int, i2: Int)
  case class IndexedRegions(idx: Int, regions: List[Region])

  case class Genotype(alleles: List[Short]) {
    def isReference: Boolean = alleles.forall(_ == 0)
    def isNoCall: Boolean = alleles.forall(_ < 0)
  }

  case class GenotypeAnnotation(dp: Int, gq: Int)

  case class PedigreeFraction(affected: Double, unaffected: Double)

  case class AlleleCombination(a1: List[Short], a2: List[Short]) {
    override def toString: String = {
      def alleleToString(a: List[Short]) =
        if (a.nonEmpty) a.mkString("/") else "v"
      s"(${alleleToString(a1)},${alleleToString(a2)})"
    }
  }

  case class GeneCounts(gene: String, count: Long)

  case class VariantCsv(contig: String,
                        pos: Int,
                        alleles: String,
                        pedigreeFractions: String,
                        externalFractions: String)

  case class ResultLineCsv(contig1: String,
                           pos1: Int,
                           contig2: String,
                           pos2: Int,
                           affectedFractions: String,
                           externalFractions: String)

}
