package nl.biopet.tools.digenicsearch

case class Variant(contig: String,
                   pos: Int,
                   alleles: List[String],
                   genotypes: List[Genotype],
                   annotations: List[AnnotationValue] = List(),
                   affectedFraction: Option[Double] = None,
                   unaffectedFraction: Option[Double] = None) {

  def addSingleFraction(pedigree: PedigreeFileArray): Variant = {
    val affectedGenotypes = pedigree.affectedArray.map(this.genotypes)
    val unaffectedGenotypes = pedigree.unaffectedArray.map(this.genotypes)

    val unaffectedFraction =
      if (unaffectedGenotypes.nonEmpty)
        unaffectedGenotypes
          .count(!_.isReference)
          .toDouble / unaffectedGenotypes.length
      else 0.0
    val affectedFraction = affectedGenotypes
      .count(!_.isReference)
      .toDouble / affectedGenotypes.length

    this.copy(unaffectedFraction = Some(unaffectedFraction),
      affectedFraction = Some(affectedFraction))
  }
}

