package nl.biopet.tools.digenicsearch

import java.io.File

import com.google.common.io.Files
import nl.biopet.utils.test.tools.ToolTest
import org.testng.annotations.Test

import scala.io.Source

class DigenicSearchTest extends ToolTest[Args] {
  def toolCommand: DigenicSearch.type = DigenicSearch
  @Test
  def testNoArgs(): Unit = {
    intercept[IllegalArgumentException] {
      DigenicSearch.main(Array())
    }
  }

  @Test
  def testDefault(): Unit = {
    val outputDir = Files.createTempDir()
    DigenicSearch.main(Array("-R",
      resourcePath("/reference.fasta"),
      "-o",
      outputDir.getAbsolutePath,
      "-i",
      resourcePath("/wgs2.vcf.gz"),
      "-p",
      resourcePath("/wgs2.ped")))

    Source.fromFile(new File(outputDir, "pairs.tsv")).getLines().length shouldBe 10
  }

  @Test
  def testRegions(): Unit = {
    val outputDir = Files.createTempDir()
    DigenicSearch.main(Array("--regions", resourcePath("/regions.bed"),"-R",
      resourcePath("/reference.fasta"),
      "-o",
      outputDir.getAbsolutePath,
      "-i",
      resourcePath("/wgs2.vcf.gz"),
      "-p",
      resourcePath("/wgs2.ped")))

    Source.fromFile(new File(outputDir, "pairs.tsv")).getLines().length shouldBe 3
  }

  @Test
  def testSingleDp(): Unit = {
    val outputDir = Files.createTempDir()
    DigenicSearch.main(Array("-R",
      resourcePath("/reference.fasta"),
      "-o",
      outputDir.getAbsolutePath,
      "-i",
      resourcePath("/wgs2.vcf.gz"),
      "-p",
      resourcePath("/wgs2.ped"),
      "--singleAnnotationFilter", "DP>=1"))

    Source.fromFile(new File(outputDir, "pairs.tsv")).getLines().length shouldBe 6

    DigenicSearch.main(Array("-R",
      resourcePath("/reference.fasta"),
      "-o",
      outputDir.getAbsolutePath,
      "-i",
      resourcePath("/wgs2.vcf.gz"),
      "-p",
      resourcePath("/wgs2.ped"),
      "--singleAnnotationFilter", "DP<=1"))

    Source.fromFile(new File(outputDir, "pairs.tsv")).getLines().length shouldBe 1

    DigenicSearch.main(Array("-R",
      resourcePath("/reference.fasta"),
      "-o",
      outputDir.getAbsolutePath,
      "-i",
      resourcePath("/wgs2.vcf.gz"),
      "-p",
      resourcePath("/wgs2.ped"),
      "--singleAnnotationFilter", "DP>=2"))

    Source.fromFile(new File(outputDir, "pairs.tsv")).getLines().length shouldBe 3
  }

  @Test
  def testPairDp(): Unit = {
    val outputDir = Files.createTempDir()
    DigenicSearch.main(Array("-R",
      resourcePath("/reference.fasta"),
      "-o",
      outputDir.getAbsolutePath,
      "-i",
      resourcePath("/wgs2.vcf.gz"),
      "-p",
      resourcePath("/wgs2.ped"),
      "--pairAnnotationFilter", "DP>=1"))

    Source.fromFile(new File(outputDir, "pairs.tsv")).getLines().length shouldBe 10

    DigenicSearch.main(Array("-R",
      resourcePath("/reference.fasta"),
      "-o",
      outputDir.getAbsolutePath,
      "-i",
      resourcePath("/wgs2.vcf.gz"),
      "-p",
      resourcePath("/wgs2.ped"),
      "--pairAnnotationFilter", "DP>=2"))

    Source.fromFile(new File(outputDir, "pairs.tsv")).getLines().length shouldBe 9
  }

  @Test
  def testDistance(): Unit = {
    val outputDir = Files.createTempDir()
    DigenicSearch.main(Array("-R",
      resourcePath("/reference.fasta"),
      "-o",
      outputDir.getAbsolutePath,
      "-i",
      resourcePath("/wgs2.vcf.gz"),
      "-p",
      resourcePath("/wgs2.ped"),
      "--maxDistance", "999"))

    Source.fromFile(new File(outputDir, "pairs.tsv")).getLines().length shouldBe 4

    DigenicSearch.main(Array("-R",
      resourcePath("/reference.fasta"),
      "-o",
      outputDir.getAbsolutePath,
      "-i",
      resourcePath("/wgs2.vcf.gz"),
      "-p",
      resourcePath("/wgs2.ped"),
      "--maxDistance", "1000"))

    Source.fromFile(new File(outputDir, "pairs.tsv")).getLines().length shouldBe 7
  }

  @Test
  def testWrongPed(): Unit = {
    val outputDir = Files.createTempDir()
    intercept[IllegalArgumentException] {
      DigenicSearch.main(Array("-R",
        resourcePath("/reference.fasta"),
        "-o",
        outputDir.getAbsolutePath,
        "-i",
        resourcePath("/wgsBoth.vcf.gz"),
        "-p",
        resourcePath("/wgs2.ped")))
    }.getMessage shouldBe "requirement failed: Sample 'wgs1' not found in ped files"
  }

  @Test
  def testBoth(): Unit = {
    val outputDir = Files.createTempDir()
    DigenicSearch.main(Array("-R",
      resourcePath("/reference.fasta"),
      "-o",
      outputDir.getAbsolutePath,
      "-i",
      resourcePath("/wgsBoth.vcf.gz"),
      "-p",
      resourcePath("/wgs1.ped"),
      "-p",
      resourcePath("/wgs2.ped")))

    Source.fromFile(new File(outputDir, "pairs.tsv")).getLines().length shouldBe 10
  }

  @Test
  def testBothAffected(): Unit = {
    val outputDir = Files.createTempDir()
    DigenicSearch.main(Array("-R",
      resourcePath("/reference.fasta"),
      "-o",
      outputDir.getAbsolutePath,
      "-i",
      resourcePath("/wgsBoth.vcf.gz"),
      "-p",
      resourcePath("/wgs1-affected.ped"),
      "-p",
      resourcePath("/wgs2.ped")))

    Source.fromFile(new File(outputDir, "pairs.tsv")).getLines().length shouldBe 0

    DigenicSearch.main(Array("-R",
      resourcePath("/reference.fasta"),
      "-o",
      outputDir.getAbsolutePath,
      "-i",
      resourcePath("/wgsBoth.vcf.gz"),
      "-p",
      resourcePath("/wgs1-affected.ped"),
      "-p",
      resourcePath("/wgs2.ped"),
      "--singleAffectedFraction", "0.5",
      "--pairAffectedFraction", "0.5"))

    Source.fromFile(new File(outputDir, "pairs.tsv")).getLines().length shouldBe 10
  }

  @Test
  def testMultiChunks(): Unit = {
    val outputDir = Files.createTempDir()
    DigenicSearch.main(Array("-R",
      resourcePath("/reference.fasta"),
      "-o",
      outputDir.getAbsolutePath,
      "-i",
      resourcePath("/wgs2.vcf.gz"),
      "-p",
      resourcePath("/wgs2.ped"),
      "--binSize", "1000", "--maxDistance", "999"))

    Source.fromFile(new File(outputDir, "pairs.tsv")).getLines().length shouldBe 4
  }
}
