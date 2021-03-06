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
import org.testng.annotations.Test

class ArgparserTest extends BiopetTest {
  @Test
  def testParseAnnotationFilter(): Unit = {
    val arg1 = ArgsParser.parseAnnotationFilter("DP>=3")
    arg1.key shouldBe "DP"
    arg1.method(3) shouldBe true
    arg1.method(4) shouldBe true
    arg1.method(2) shouldBe false

    val arg2 = ArgsParser.parseAnnotationFilter("DP<=3")
    arg2.key shouldBe "DP"
    arg2.method(3) shouldBe true
    arg2.method(4) shouldBe false
    arg2.method(2) shouldBe true

    intercept[IllegalArgumentException] {
      ArgsParser.parseAnnotationFilter("DP++++3")
    }.getMessage shouldBe "No method found, possible methods: >=, <="
  }

  @Test
  def testMaxContigsInSingleJob(): Unit = {
    val args = Array("-R",
                     resourcePath("/reference.fasta"),
                     "-o",
                     "./",
                     "-i",
                     resourcePath("/wgs2.vcf.gz"),
                     "-p",
                     resourcePath("/wgs2.ped"),
                     "--maxContigsInSingleJob",
                     "300")

    DigenicSearch.cmdArrayToArgs(args).maxContigsInSingleJob shouldBe 300
  }

  @Test
  def testDetectionMode(): Unit = {
    def args(mode: String) =
      Array("-R",
            resourcePath("/reference.fasta"),
            "-o",
            "./",
            "-i",
            resourcePath("/wgs2.vcf.gz"),
            "-p",
            resourcePath("/wgs2.ped"),
            "--detectionMode",
            mode)

    DigenicSearch
      .cmdArrayToArgs(args("Allele"))
      .detectionMode shouldBe DetectionMode.Allele
    DigenicSearch
      .cmdArrayToArgs(args("allele"))
      .detectionMode shouldBe DetectionMode.Allele

    intercept[IllegalArgumentException] {
      DigenicSearch.cmdArrayToArgs(args("sadfhaslf"))
    }
  }
}
