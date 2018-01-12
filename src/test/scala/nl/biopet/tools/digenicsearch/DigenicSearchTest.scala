package nl.biopet.tools.digenicsearch

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
    DigenicSearch.main(Array("-R",
      "/exports/sasc/testing/fixtures/reference/bwa/reference.fasta",
      "-o",
      "/home/pjvan_thof/test/digenic",
      "-i",
      "/exports/sasc/testing/fixtures/samples/wgs2/wgs2.vcf.gz",
//      "--pairAnnotationFilter",
//      "DP>=10",
      "--binSize", "1000", "--maxDistance", "999"))
  }
}
