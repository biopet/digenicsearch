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
}
