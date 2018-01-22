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
      resourcePath("/wgs2.ped"), "--maxContigsInSingleJob", "300")

    DigenicSearch.cmdArrayToArgs(args).maxContigsInSingleJob shouldBe 300
  }

  @Test
  def testDetectionMode(): Unit = {
    val args = Array("-R",
      resourcePath("/reference.fasta"),
      "-o",
      "./",
      "-i",
      resourcePath("/wgs2.vcf.gz"),
      "-p",
      resourcePath("/wgs2.ped"), "--detectionMode", "Allele")

    DigenicSearch.cmdArrayToArgs(args).detectionMode shouldBe DetectionMode.Allele
  }
}
