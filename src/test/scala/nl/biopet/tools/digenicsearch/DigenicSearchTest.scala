package nl.biopet.tools.digenicsearch

import java.io.File

import com.google.common.io.Files
import nl.biopet.utils.test.tools.ToolTest
import org.testng.annotations.Test
import nl.biopet.utils.conversions.yamlFileToMap

class DigenicSearchTest extends ToolTest[Args] {

  System.setProperty("spark.sql.shuffle.partitions", "1")

  def toolCommand: DigenicSearch.type = DigenicSearch
  @Test
  def testNoArgs(): Unit = {
    intercept[IllegalArgumentException] {
      DigenicSearch.main(Array())
    }
  }

  def referenceArgs = Array("-R", resourcePath("/reference.fasta"))
  def regionsArgs = Array("--regions", resourcePath("/regions.bed"))
  def wgs2Arg = Array("-i", resourcePath("/wgs2.vcf.gz"))
  def wgs2PedArg = Array("-p", resourcePath("/wgs2.ped"))
  def outputDirArg(file: File) = Array("-o", file.getAbsolutePath)
  def defaultWgs2Arg(outputDir: File): Array[String] = outputDirArg(outputDir) ++ referenceArgs ++ wgs2Arg ++ wgs2PedArg

  @Test
  def testDefault(): Unit = {
    val outputDir = Files.createTempDir()
    DigenicSearch.main(defaultWgs2Arg(outputDir))

    val stats = yamlFileToMap(new File(outputDir, "stats.yml"))
    stats("total_pairs") shouldBe 10
  }

  @Test
  def testRegions(): Unit = {
    val outputDir = Files.createTempDir()
    DigenicSearch.main(defaultWgs2Arg(outputDir) ++ regionsArgs)

    val stats = yamlFileToMap(new File(outputDir, "stats.yml"))
    stats("total_pairs") shouldBe 3
  }

  @Test
  def testSingleDp(): Unit = {
    val outputDir = Files.createTempDir()
    DigenicSearch.main(defaultWgs2Arg(outputDir) ++ Array("--singleAnnotationFilter", "DP>=1"))

    val stats = yamlFileToMap(new File(outputDir, "stats.yml"))
    stats("total_pairs") shouldBe 6

    val outputDir2 = Files.createTempDir()
    DigenicSearch.main(defaultWgs2Arg(outputDir2) ++ Array("--singleAnnotationFilter", "DP<=1"))

    val stats2 = yamlFileToMap(new File(outputDir2, "stats.yml"))
    stats2("total_pairs") shouldBe 1

    val outputDir3 = Files.createTempDir()
    DigenicSearch.main(defaultWgs2Arg(outputDir3) ++ Array("--singleAnnotationFilter", "DP>=2"))

    val stats3 = yamlFileToMap(new File(outputDir3, "stats.yml"))
    stats3("total_pairs") shouldBe 3
  }

  @Test
  def testPairDp(): Unit = {
    val outputDir = Files.createTempDir()
    DigenicSearch.main(defaultWgs2Arg(outputDir) ++ Array("--pairAnnotationFilter", "DP>=1"))

    val stats = yamlFileToMap(new File(outputDir, "stats.yml"))
    stats("total_pairs") shouldBe 10

    val outputDir2 = Files.createTempDir()
    DigenicSearch.main(defaultWgs2Arg(outputDir2) ++ Array("--pairAnnotationFilter", "DP>=2"))

    val stats2 = yamlFileToMap(new File(outputDir2, "stats.yml"))
    stats2("total_pairs") shouldBe 9
  }

  @Test
  def testDistance(): Unit = {
    val outputDir = Files.createTempDir()
    DigenicSearch.main(defaultWgs2Arg(outputDir) ++ Array("--maxDistance", "999"))

    val stats = yamlFileToMap(new File(outputDir, "stats.yml"))
    stats("total_pairs") shouldBe 4

    val outputDir2 = Files.createTempDir()
    DigenicSearch.main(defaultWgs2Arg(outputDir2) ++ Array("--maxDistance", "1000"))

    val stats2 = yamlFileToMap(new File(outputDir2, "stats.yml"))
    stats2("total_pairs") shouldBe 7
  }

  @Test
  def testWrongPed(): Unit = {
    val outputDir = Files.createTempDir()
    intercept[IllegalArgumentException] {
      DigenicSearch.main(outputDirArg(outputDir) ++ referenceArgs ++ wgs2PedArg ++ Array(
        "-i", resourcePath("/wgsBoth.vcf.gz")))
    }.getMessage shouldBe "requirement failed: Sample 'wgs1' not found in ped files"
  }

  @Test
  def testBoth(): Unit = {
    val outputDir = Files.createTempDir()
    DigenicSearch.main(outputDirArg(outputDir) ++ referenceArgs ++ wgs2PedArg ++ Array(
      "-i", resourcePath("/wgsBoth.vcf.gz"), "-p", resourcePath("/wgs1.ped")))

    val stats = yamlFileToMap(new File(outputDir, "stats.yml"))
    stats("total_pairs") shouldBe 10
  }

  @Test
  def testBothAffected(): Unit = {
    val outputDir = Files.createTempDir()
    DigenicSearch.main(outputDirArg(outputDir) ++ referenceArgs ++ wgs2PedArg ++ Array(
      "-i", resourcePath("/wgsBoth.vcf.gz"), "-p", resourcePath("/wgs1-affected.ped")))

    val stats = yamlFileToMap(new File(outputDir, "stats.yml"))
    stats("total_pairs") shouldBe 0

    val outputDir2 = Files.createTempDir()
    DigenicSearch.main(outputDirArg(outputDir2) ++ referenceArgs ++ wgs2PedArg ++ Array(
      "-i", resourcePath("/wgsBoth.vcf.gz"), "-p", resourcePath("/wgs1-affected.ped"),
      "--singleAffectedFraction", "0.5",
      "--pairAffectedFraction", "0.5"))

    val stats2 = yamlFileToMap(new File(outputDir2, "stats.yml"))
    stats2("total_pairs") shouldBe 10
  }

  @Test
  def testMultiChunks(): Unit = {
    val outputDir = Files.createTempDir()
    DigenicSearch.main(defaultWgs2Arg(outputDir) ++ Array(
      "--binSize", "1000",
      "--maxDistance", "999"))

    val stats = yamlFileToMap(new File(outputDir, "stats.yml"))
    stats("total_pairs") shouldBe 4
  }
}
