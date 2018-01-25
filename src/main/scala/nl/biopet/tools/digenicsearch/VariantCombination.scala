package nl.biopet.tools.digenicsearch

import nl.biopet.tools.digenicsearch.DetectionMode.DetectionResult

case class VariantCombination(v1: Variant,
                              v2: Variant,
                              alleles: List[AlleleCombination]) {
  def cleanResults: VariantCombination = {
    cleanResults(alleles)
  }

  def cleanResults(keep: List[AlleleCombination]): VariantCombination = {
    val alleles1 = keep.map(_.a1).toSet
    val alleles2 = keep.map(_.a2).toSet
    val detectionResult1 = DetectionResult(v1.detectionResult.result.filter { case (a, _) => alleles1.contains(a) })
    val externalDetectionResult1 = v1.externalDetectionResult.map(x => DetectionResult(x.result.filter { case (a, _) => alleles1.contains(a) }))
    val newV1 = v1.copy(
      detectionResult = detectionResult1,
      externalDetectionResult = externalDetectionResult1
    )
    val detectionResult2 = DetectionResult(v2.detectionResult.result.filter { case (a, _) => alleles2.contains(a) })
    val externalDetectionResult2 = v2.externalDetectionResult.map(x => DetectionResult(x.result.filter { case (a, _) => alleles2.contains(a) }))
    val newV2 = v2.copy(
      detectionResult = detectionResult2,
      externalDetectionResult = externalDetectionResult2
    )
    this.copy(alleles = keep, v1 = newV1, v2 = newV2)
  }
}
