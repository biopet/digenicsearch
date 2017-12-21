package nl.biopet.tools

package object digenicsearch {
  case class Region(contig: String, start: Int, end: Int)

  case class Variant(contig: String,
                     pos: Int,
                     alleles: Array[String],
                     genotypes: Array[Genotype],
                     annotations: Array[(String, Array[Double])] = Array())

  case class Genotype(alleles: Array[Short], dp: Int, gq: Int)

  case class VariantList(idx: Int, variants: Array[Variant])

  case class Idx(i: Int, j: Int)
  case class IdxWithVariant(i: Int, j: Int, v1: VariantList)
  case class IdxWithVariants(i: Int, j: Int, v1: VariantList, v2: VariantList)

}
