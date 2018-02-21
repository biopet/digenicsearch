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

import nl.biopet.test.BiopetTest
import nl.biopet.tools.digenicsearch.DetectionMode.DetectionResult
import org.testng.annotations.Test

class DetectionModeTest extends BiopetTest {
  @Test
  def testVariant(): Unit = {
    DetectionMode.Varant.method(List(Genotype(List(0, 1)))) shouldBe DetectionResult(
      List(Nil -> List(true)))
    DetectionMode.Varant.method(List(Genotype(List(1, 1)))) shouldBe DetectionResult(
      List(Nil -> List(true)))
    DetectionMode.Varant.method(List(Genotype(List(0, 0)))) shouldBe DetectionResult(
      List(Nil -> List(false)))
  }

  @Test
  def testAllele(): Unit = {
    DetectionMode.Allele.method(List(Genotype(List(0, 1)))) shouldBe DetectionResult(
      List(List(1.toShort) -> List(true)))
    DetectionMode.Allele.method(List(Genotype(List(1, 1)))) shouldBe DetectionResult(
      List(List(1.toShort) -> List(true)))
    DetectionMode.Allele.method(List(Genotype(List(0, 0)))) shouldBe DetectionResult(
      List())
  }

  @Test
  def testGenotype(): Unit = {
    DetectionMode.Genotype.method(List(Genotype(List(0, 1)))) shouldBe DetectionResult(
      List(List(0.toShort, 1.toShort) -> List(true)))
    DetectionMode.Genotype.method(List(Genotype(List(1, 1)))) shouldBe DetectionResult(
      List(List(1.toShort, 1.toShort) -> List(true)))
    DetectionMode.Genotype.method(List(Genotype(List(0, 0)))) shouldBe DetectionResult(
      List())
  }
}
