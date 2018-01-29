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

import scala.language.implicitConversions

object DetectionMode extends Enumeration {
  case class DetectionResult(result: List[(List[Short], List[Boolean])])

  protected case class Val(method: List[Genotype] => DetectionResult)
      extends super.Val
  implicit def valueToVal(x: Value): Val = x.asInstanceOf[Val]

  val Varant = Val { alleles =>
    DetectionResult(
      List(Nil -> alleles.map(genotype => !genotype.isReference)))
  }

  val Allele = Val { alleles =>
    DetectionResult(
      alleles.flatMap(_.alleles).filter(_ != 0).distinct.map { key =>
        List(key) -> alleles.map(_.alleles.contains(key))
      })
  }

  val Genotype = Val { alleles =>
    DetectionResult(
      alleles.filter(!_.isReference).map(_.alleles).distinct.map { g =>
        g -> alleles.map(_.alleles == g)
      })
  }
}
