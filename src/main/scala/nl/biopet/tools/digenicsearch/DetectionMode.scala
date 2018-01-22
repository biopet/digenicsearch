package nl.biopet.tools.digenicsearch

object DetectionMode extends Enumeration {
  case class DetectionResult(result: List[(List[Short], List[Boolean])])

  protected case class Val(method: List[Genotype] => DetectionResult) extends super.Val
  implicit def valueToVal(x: Value): Val = x.asInstanceOf[Val]

  val Varant = Val { alleles =>
    DetectionResult(List(Nil -> alleles.map(genotype => !genotype.isReference)))
  }

  val Allele = Val { alleles => ???
    //TODO
  }

  val Genotype = Val { alleles => ???
    //TODO
  }

}
