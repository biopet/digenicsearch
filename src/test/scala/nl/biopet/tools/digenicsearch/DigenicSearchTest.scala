package nl.biopet.tools.digenicsearch

import com.google.common.io.Files
import nl.biopet.utils.test.tools.ToolTest
import org.testng.annotations.Test

class DigenicSearchTest extends ToolTest[Args] {
  def toolCommand: DigenicSearch.type = DigenicSearch
  @Test
  def testNoArgs(): Unit = {
    intercept[IllegalArgumentException] {
      DigenicSearch.main(Array())
    }
  }

  @Test
  def test(): Unit = {
    val outputDir = Files.createTempDir()
    DigenicSearch.main(Array("-R",
      resourcePath("/reference.fasta"),
      "-o",
      outputDir.getAbsolutePath,
      "-i",
      resourcePath("/wgs2.vcf.gz"),
//      "--pairAnnotationFilter",
//      "DP>=10",
      "--binSize", "1000", "--maxDistance", "999"))
  }
}
