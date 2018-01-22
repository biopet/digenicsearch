package nl.biopet.tools.digenicsearch

import nl.biopet.tools.digenicsearch.DetectionMode.DetectionResult

case class Variant(contig: String,
                   pos: Int,
                   alleles: List[String],
                   genotypes: List[Genotype],
                   annotations: List[AnnotationValue] = List(),
                   genotypeAnnotation: List[GenotypeAnnotation],
                   detectionResult: DetectionMode.DetectionResult) {

  def filterSingleFraction(pedigree: PedigreeFileArray, cutoffs: FractionsCutoffs): Option[Variant] = {

    val alleles = detectionResult.result.toMap

    val result = for ((allele, result) <- alleles) yield {
      val affectedGenotypes = pedigree.affectedArray.map(result)
      val unaffectedGenotypes = pedigree.unaffectedArray.map(result)

      val unaffectedFraction =
        if (unaffectedGenotypes.nonEmpty) {
          unaffectedGenotypes.count(_ == true).toDouble / unaffectedGenotypes.length
        } else 0.0
      val affectedFraction = affectedGenotypes.count(_ == true).toDouble / affectedGenotypes.length

      allele -> PedigreeFraction(affectedFraction, unaffectedFraction)
    }

    val filter = result
      .filter(_._2.unaffected <= cutoffs.singleUnaffectedFraction)
      .filter(_._2.affected >= cutoffs.singleAffectedFraction)

    if (filter.nonEmpty) {
      Some(this.copy(detectionResult = DetectionResult(filter.keys.map(k => k -> alleles(k)).toList)))
    } else None
  }
}

