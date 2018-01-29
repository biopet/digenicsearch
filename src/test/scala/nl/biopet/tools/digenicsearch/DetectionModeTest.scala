package nl.biopet.tools.digenicsearch

import nl.biopet.test.BiopetTest
import nl.biopet.tools.digenicsearch.DetectionMode.DetectionResult
import org.testng.annotations.Test

class DetectionModeTest extends BiopetTest {
  @Test
  def testVariant(): Unit = {
    DetectionMode.Varant.method(List(Genotype(List(0,1)))) shouldBe DetectionResult(List(Nil -> List(true)))
    DetectionMode.Varant.method(List(Genotype(List(1,1)))) shouldBe DetectionResult(List(Nil -> List(true)))
    DetectionMode.Varant.method(List(Genotype(List(0,0)))) shouldBe DetectionResult(List(Nil -> List(false)))
  }

  @Test
  def testAllele(): Unit = {
    DetectionMode.Allele.method(List(Genotype(List(0,1)))) shouldBe DetectionResult(List(List(1.toShort) -> List(true)))
    DetectionMode.Allele.method(List(Genotype(List(1,1)))) shouldBe DetectionResult(List(List(1.toShort) -> List(true)))
    DetectionMode.Allele.method(List(Genotype(List(0,0)))) shouldBe DetectionResult(List())
  }

  @Test
  def testGenotype(): Unit = {
    DetectionMode.Genotype.method(List(Genotype(List(0,1)))) shouldBe DetectionResult(List(List(0.toShort, 1.toShort) -> List(true)))
    DetectionMode.Genotype.method(List(Genotype(List(1,1)))) shouldBe DetectionResult(List(List(1.toShort, 1.toShort) -> List(true)))
    DetectionMode.Genotype.method(List(Genotype(List(0,0)))) shouldBe DetectionResult(List())
  }
}
