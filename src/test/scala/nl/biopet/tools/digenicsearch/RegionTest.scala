package nl.biopet.tools.digenicsearch

import nl.biopet.test.BiopetTest
import org.testng.annotations.Test

class RegionTest extends BiopetTest {
  @Test
  def testDistance(): Unit = {
    val r1 = Region(1, 1, 2)
    val r2 = Region(1, 4, 5)
    val r3 = Region(2, 4, 5)

    r1.distance(r2) shouldBe Some(2)
    r2.distance(r1) shouldBe Some(2)
    r1.distance(r3) shouldBe None
    r2.distance(r3) shouldBe None
  }
}
