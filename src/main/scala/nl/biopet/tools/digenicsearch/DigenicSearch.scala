package nl.biopet.tools.digenicsearch

import java.io.File

import htsjdk.variant.variantcontext.{Allele, VariantContext}
import htsjdk.variant.vcf.VCFFileReader
import nl.biopet.utils.ngs.fasta
import nl.biopet.utils.ngs.intervals.{BedRecord, BedRecordList}
import nl.biopet.utils.spark
import nl.biopet.utils.ngs.vcf
import nl.biopet.utils.tool.ToolCommand
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.api.java.function.FilterFunction
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SparkSession}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
//import org.bdgenomics.adam.models.{ReferenceRegion, SequenceDictionary}
//import org.bdgenomics.adam.rdd.ADAMContext._

import scala.concurrent.ExecutionContext.Implicits.global

import scala.collection.JavaConversions._

object DigenicSearch extends ToolCommand[Args] {
  def emptyArgs = Args()
  def argsParser = new ArgsParser(this)

  def main(args: Array[String]): Unit = {
    val cmdArgs = cmdArrayToArgs(args)

    logger.info("Start")
//    val sparkConf: SparkConf = spark.getConf(toolName, cmdArgs.sparkMaster, cmdArgs.sparkConfigValues)
    val sparkConf: SparkConf = new SparkConf(true)
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import sparkSession.implicits._
    implicit val sc: SparkContext = sparkSession.sparkContext
    println(s"Context is up, see ${sc.uiWebUrl.getOrElse("")}")

    val samples = sc.broadcast(cmdArgs.inputFiles.flatMap(vcf.getSampleIds).toArray)
    require(samples.value.lengthCompare(samples.value.distinct.length) == 0, "Duplicated samples detected")

//    val dict = SequenceDictionary.fromSAMSequenceDictionary(fasta.getCachedDict(cmdArgs.reference))

    val annotations: Broadcast[Set[String]] = sc.broadcast(cmdArgs.singleAnnotationFilter.map(_._1).toSet ++ cmdArgs.pairAnnotationFilter.map(_._1).toSet)

    val singleFilters = sc.broadcast(cmdArgs.singleAnnotationFilter)
    val pairFilters = sc.broadcast(cmdArgs.pairAnnotationFilter)
    val inputFiles = sc.broadcast(cmdArgs.inputFiles)
    val regions = generateRegions(cmdArgs).toArray
    val regionsRdd = sc.parallelize(regions, regions.size)
    val blablabla = regions.zipWithIndex.map(r => r._2 -> sc.parallelize(Seq(r._1), 1).mapPartitions { it =>
      val readers = inputFiles.value.map(new VCFFileReader(_))
      it.map { region =>
        loadRegion(readers, region, samples, annotations).toList
      }
    }.cache()).toMap

    val futures2: Seq[Future[Long]] = for (i <- 0 to regions.length; j <- i to regions.length) yield {
      val rdd = blablabla(i).union(blablabla(j)).repartition(1)
      rdd.mapPartitions { bla =>
        val list1 = bla.next()
        val list2 = bla.next()
        for (v1 <- list1.toIterator; v2 <- list2) yield {
          (v1, v2)
        }
        //Iterator(bla.size)
      }.countAsync()
    }

    println(Await.result(Future.sequence(futures2), Duration.Inf).sum)

    sys.exit()

    val variants = regionsRdd.mapPartitions { it =>
      val readers = inputFiles.value.map(new VCFFileReader(_))
      it.flatMap { region =>
        loadRegion(readers, region, samples, annotations)
      }
    }
    val singleFilter = variants
      .filter { v =>
      singleFilters.value.isEmpty ||
      singleFilters.value.forall { c =>
        v.annotations.find(_._1 == c._1).get._2.forall(c._2)
      }
    }.repartition(250)

    val idxVariant = singleFilter.mapPartitionsWithIndex { case (idx, it) =>
      Iterator(VariantList(idx, it.toArray))
    }.toDS().cache()

    val futures: Seq[Future[Long]] = for (i <- 1 until 250; j <- i until 250) yield {
      val v1 = idxVariant.filter(idxVariant("idx") === i)
      val v2 = idxVariant.filter(idxVariant("idx") === j)

      val rdd = v1.union(v2).repartition(1)
      rdd.mapPartitions { bla =>
        val list1 = bla.next()
        val list2 = bla.next()
        for (v1 <- list1.variants.toIterator; v2 <- list2.variants) yield {
          (v1, v2)
        }
        //Iterator(bla.size)
      }.rdd.countAsync()
//      println(count)
//      Future(count)
//      rdd.map { case (v1, v2) =>
//        (for (r1 <- v1.variants; r2 <- v2.variants if r1.contig != r2.contig || r1.pos < r2.pos) yield {
//
//        }).length.toLong
//      }.countAsync()
    }

    val bla = Await.result(Future.sequence(futures), Duration.Inf).sum

//    val bla = idxDs.joinWith(idxVariant, idxVariant("idx") === idxDs("i"))
//      .map(x => IdxWithVariant(x._1.i, x._1.j, x._2))
//    val bla2 = bla.joinWith(idxVariant2, idxVariant2("idx") === bla("j"))
//
//    val bla3 = bla2.flatMap { case (list1, list2) =>
//      for {
//        v1 <- list1.v1.variants
//        v2 <- list2.variants
//        if v1.contig != v2.contig || v1.pos < v2.pos
//      } yield (v1, v2)
//    }
//    val ds = singleFilter.toDS().cache()
//    println(bla3.count())

    Thread.sleep(1000000)
    sc.stop()

    logger.info("Done")
  }

  def loadRegion(inputReaders: List[VCFFileReader],
                 region: Region,
                 samples: Broadcast[Array[String]],
                 annotationsFields: Broadcast[Set[String]]): Iterator[Variant] = {
    val bedRecord = BedRecord(region.contig, region.start, region.end)
    new Iterator[Variant] {
      protected val iterators: List[BufferedIterator[VariantContext]] =
        inputReaders.map(vcf.loadRegion(_, bedRecord)).map(_.buffered)

      def hasNext: Boolean = iterators.exists(_.hasNext)

      def next(): Variant = {
        val position = iterators.filter(_.hasNext).map(_.head.getStart).min
        val records = iterators.filter(_.hasNext).filter(_.head.getStart == position).map(_.next())
        val allAlleles = records.flatMap(_.getAlleles)
        val refAlleles = allAlleles.filter(_.isReference)
        val annotations = annotationsFields.value.map { field =>
          field -> records.flatMap { record =>
            if (record.hasAttribute(field))
              Some(record.getAttributeAsDouble(field, 0.0))
            else None
          }.toArray
        }.toArray
        val alleles: Array[String] = if (refAlleles.length > 1) {
          throw new UnsupportedOperationException("Multiple reference alleles found")
        } else allAlleles.map(_.toString).toArray
        val genotypes = samples.value.map { sampleId =>
          val bla = records.flatMap(x => Option(x.getGenotype(sampleId)))
          val g = bla.head.getAlleles.map(x => alleles.indexOf(x.toString).toShort).toArray
          Genotype(g, bla.head.getDP, bla.head.getDP)
        }
        Variant(bedRecord.chr, position, alleles, genotypes, annotations)
      }
    }
  }

  /** creates regions to analyse */
  def generateRegions(cmdArgs: Args): List[Region] = {
    val regions = (cmdArgs.regions match {
      case Some(i) =>
        BedRecordList.fromFile(i).validateContigs(cmdArgs.reference)
      case _ => BedRecordList.fromReference(cmdArgs.reference)
    }).combineOverlap
      .scatter(cmdArgs.binSize,
        maxContigsInSingleJob = Some(cmdArgs.maxContigsInSingleJob))
      .flatten
    regions.map(x => Region(x.chr, x.start, x.end))
  }

  def descriptionText: String =
    """
      |jfhgkjsdhgkjshdfjkvbsjkldvksdfkvjsd bjksbfkg bkdfb kbsfdkjg bdf sjhsdfb sbdj gbsfd jfhgkjsdhgkjshdfjkvbsjkldvksdfkvjsd bjksbfkg bkdfb kbsfdkjg bdf sjhsdfb sbdj gbsfd jfhgkjsdhgkjshdfjkvbsjkldvksdfkvjsd bjksbfkg bkdfb kbsfdkjg bdf sjhsdfb sbdj gbsfd jfhgkjsdhgkjshdfjkvbsjkldvksdfkvjsd bjksbfkg bkdfb kbsfdkjg bdf sjhsdfb sbdj gbsfd jfhgkjsdhgkjshdfjkvbsjkldvksdfkvjsd bjksbfkg bkdfb kbsfdkjg bdf sjhsdfb sbdj gbsfd jfhgkjsdhgkjshdfjkvbsjkldvksdfkvjsd bjksbfkg bkdfb kbsfdkjg bdf sjhsdfb sbdj gbsfd jfhgkjsdhgkjshdfjkvbsjkldvksdfkvjsd bjksbfkg bkdfb kbsfdkjg bdf sjhsdfb sbdj gbsfd
    """.stripMargin

  def manualText: String =
    """
      |jfhgkjsdhgkjshdfjkvbsjkldvksdfkvjsd bjksbfkg bkdfb kbsfdkjg bdf sjhsdfb sbdj gbsfd jfhgkjsdhgkjshdfjkvbsjkldvksdfkvjsd bjksbfkg bkdfb kbsfdkjg bdf sjhsdfb sbdj gbsfd jfhgkjsdhgkjshdfjkvbsjkldvksdfkvjsd bjksbfkg bkdfb kbsfdkjg bdf sjhsdfb sbdj gbsfd jfhgkjsdhgkjshdfjkvbsjkldvksdfkvjsd bjksbfkg bkdfb kbsfdkjg bdf sjhsdfb sbdj gbsfd jfhgkjsdhgkjshdfjkvbsjkldvksdfkvjsd bjksbfkg bkdfb kbsfdkjg bdf sjhsdfb sbdj gbsfd jfhgkjsdhgkjshdfjkvbsjkldvksdfkvjsd bjksbfkg bkdfb kbsfdkjg bdf sjhsdfb sbdj gbsfd jfhgkjsdhgkjshdfjkvbsjkldvksdfkvjsd bjksbfkg bkdfb kbsfdkjg bdf sjhsdfb sbdj gbsfd
    """.stripMargin

  def exampleText: String =
    """
      |jfhgkjsdhgkjshdfjkvbsjkldvksdfkvjsd bjksbfkg bkdfb kbsfdkjg bdf sjhsdfb sbdj gbsfd jfhgkjsdhgkjshdfjkvbsjkldvksdfkvjsd bjksbfkg bkdfb kbsfdkjg bdf sjhsdfb sbdj gbsfd jfhgkjsdhgkjshdfjkvbsjkldvksdfkvjsd bjksbfkg bkdfb kbsfdkjg bdf sjhsdfb sbdj gbsfd jfhgkjsdhgkjshdfjkvbsjkldvksdfkvjsd bjksbfkg bkdfb kbsfdkjg bdf sjhsdfb sbdj gbsfd jfhgkjsdhgkjshdfjkvbsjkldvksdfkvjsd bjksbfkg bkdfb kbsfdkjg bdf sjhsdfb sbdj gbsfd jfhgkjsdhgkjshdfjkvbsjkldvksdfkvjsd bjksbfkg bkdfb kbsfdkjg bdf sjhsdfb sbdj gbsfd jfhgkjsdhgkjshdfjkvbsjkldvksdfkvjsd bjksbfkg bkdfb kbsfdkjg bdf sjhsdfb sbdj gbsfd
    """.stripMargin
}
