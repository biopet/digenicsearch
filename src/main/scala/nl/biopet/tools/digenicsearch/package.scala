package nl.biopet.tools

package object digenicsearch {
  case class Region(contig: String, start: Int, end: Int) {
    def distance(other: Region): Option[Long] = {
      if (this.contig == other.contig) {
        if (this.start > other.end) Some(this.start - other.end)
        else if (other.start > this.end) Some(other.start - this.end)
        else Some(0)
      } else None
    }
  }

  case class Variant(contig: String,
                     pos: Int,
                     alleles: List[String],
                     genotypes: List[Genotype],
                     annotations: List[(String, List[Double])] = List())

  case class Genotype(alleles: List[Short], dp: Int, gq: Int)

  case class VariantList(idx: Int, variants: Array[Variant])

  case class Idx(i: Int, j: Int)
  case class IdxWithVariant(i: Int, j: Int, v1: VariantList)
  case class IdxWithVariants(i: Int, j: Int, v1: VariantList, v2: VariantList)

}
