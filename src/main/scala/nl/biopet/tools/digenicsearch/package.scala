package nl.biopet.tools

package object digenicsearch {
  case class Region(contig: String, start: Int, end: Int)

  case class Variant(contig: String,
                     pos: Int,
                     alleles: Array[String],
                     genotypes: Array[Genotype],
                     anotations: Map[String, String] = Map(),
                     dummy: Boolean = true)

  case class Genotype(alleles: Array[Short], dp: Option[Int] = None, gq: Option[Int] = None)

}
