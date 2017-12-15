package nl.biopet.tools.digenicsearch

import java.io.File

import htsjdk.variant.variantcontext.{Allele, VariantContext}
import htsjdk.variant.vcf.VCFFileReader
import nl.biopet.utils.ngs.fasta
import nl.biopet.utils.ngs.intervals.{BedRecord, BedRecordList}
import nl.biopet.utils.spark
import nl.biopet.utils.ngs.vcf
import nl.biopet.utils.tool.ToolCommand
import org.apache.spark.SparkContext
import org.apache.spark.api.java.function.FilterFunction
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SparkSession}
//import org.bdgenomics.adam.models.{ReferenceRegion, SequenceDictionary}
//import org.bdgenomics.adam.rdd.ADAMContext._

import scala.collection.JavaConversions._

object DigenicSearch extends ToolCommand[Args] {
  def emptyArgs = Args()
  def argsParser = new ArgsParser(this)

  def main(args: Array[String]): Unit = {
    val cmdArgs = cmdArrayToArgs(args)

    logger.info("Start")
    val sparkConf = spark.getConf(toolName, cmdArgs.sparkMaster, cmdArgs.sparkConfigValues)
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import sparkSession.implicits._
    implicit val sc: SparkContext = sparkSession.sparkContext
    println(s"Context is up, see ${sc.uiWebUrl.getOrElse("")}")

    val samples = sc.broadcast(cmdArgs.inputFiles.flatMap(vcf.getSampleIds).toArray)
    require(samples.value.lengthCompare(samples.value.distinct.size) == 0, "Duplicated samples detected")

//    val dict = SequenceDictionary.fromSAMSequenceDictionary(fasta.getCachedDict(cmdArgs.reference))

    val inputFiles = sc.broadcast(cmdArgs.inputFiles)
    val regions = generateRegions(cmdArgs)
    val regionsRdd = sparkSession.createDataset(sc.parallelize(regions, regions.size))

    val variants = regionsRdd.mapPartitions { it =>
      val readers = inputFiles.value.map(new VCFFileReader(_))
      it.flatMap { region =>
        loadRegion(readers, region, samples)
      }
    }

    val second = variants.as("variants2")
    val cross = variants.joinWith(second, variants("dummy") === second("dummy"), "cross")

    val filter = cross.filter { v =>
      v._1.genotypes.forall(_.alleles.forall(_ != 0)) && v._2.genotypes.forall(_.alleles.forall(_ != 0))
    }

    val count = filter.count()

    println(cross.count())
    println(count)

    Thread.sleep(1000000)
    sc.stop()

    logger.info("Done")
  }

  def loadRegion(inputReaders: List[VCFFileReader],
                 region: Region,
                 samples: Broadcast[Array[String]]): Iterator[Variant] = {
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
        val alleles: Array[String] = if (refAlleles.size > 1) {
          throw new UnsupportedOperationException("Multiple reference alleles found")
        } else allAlleles.map(_.toString).toArray
        val genotypes = samples.value.map { sampleId =>
          val bla = records.flatMap(x => Option(x.getGenotype(sampleId)))
          val g = bla.head.getAlleles.map(x => alleles.indexOf(x.toString).toShort).toArray
          Genotype(g)
        }
        Variant(bedRecord.chr, position, alleles, genotypes)
      }
    }
  }

  case class Region(contig: String, start: Int, end: Int)

  case class Variant(contig: String, pos: Int, alleles: Array[String], genotypes: Array[Genotype], dummy: Boolean = true)

  case class Genotype(alleles: Array[Short], dp: Option[Int] = None, gq: Option[Int] = None)

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
