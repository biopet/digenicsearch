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
  case class Region(contig: String, start: Int, end: Int) {
    def distance(other: Region): Option[Long] = {
      if (this.contig == other.contig) {
        if (this.start > other.end) Some(this.start - other.end)
        else if (other.start > this.end) Some(other.start - this.end)
        else Some(0)
      } else None
    }
  }

  case class AnnotationFilter(key: String, method: Double => Boolean)

  case class AnnotationValue(key: String, value: List[Double])
  case class Variant(contig: String,
                     pos: Int,
                     alleles: List[String],
                     genotypes: List[Genotype],
                     annotations: List[AnnotationValue] = List())

  case class Genotype(alleles: List[Short], dp: Int, gq: Int) {
    def isReference: Boolean = alleles.forall(_ == 0)
  }

}
