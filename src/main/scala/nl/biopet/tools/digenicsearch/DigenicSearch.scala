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
    val sparkConf: SparkConf = new SparkConf(true).setMaster(cmdArgs.sparkMaster.getOrElse("local[1]"))
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
    val regionsRdds = regions.zipWithIndex.map(r => r._2 -> sc.parallelize(Seq(r._1), 1).mapPartitions { it =>
      val readers = inputFiles.value.map(new VCFFileReader(_))
      it.map { region =>
        region.flatMap(loadRegion(readers, _, samples, annotations).toList)
          .filter { v =>
          singleFilters.value.isEmpty ||
            singleFilters.value.forall { c =>
              v.annotations.find(_._1 == c._1).get._2.forall(c._2)
            }
        }
      }
    }.cache()).toMap

    val futures2 = for (i <- regions.indices; j <- i until regions.length if (toFarAway(regions(i), regions(j), cmdArgs.maxDistance))) yield {
      val same = i == j
      val rdd = regionsRdds(i)
        .union(regionsRdds(j))
        .repartition(1)
      rdd.mapPartitions { records =>
        val list1 = records.next()
        val list2 = records.next()
        (for {
          v1 <- list1.zipWithIndex.toIterator
          v2 <- list2.zipWithIndex
          if !same || v1._2 < v2._2
        } yield {
          val sorted = List(v1._1, v2._1).sortBy(v => (v.contig, v.pos))
          (sorted(0), sorted(1))
        }).filter { case (v1, v2) =>
          cmdArgs.maxDistance match {
            case Some(distance) if v1.contig != v2.contig => false
            case Some(distance) => (v1.pos - v2.pos).abs <= distance
            case _ => true
          }
        }.filter { case (v1, v2) =>
          val list = List(v1, v2)
          pairFilters.value.isEmpty ||
            pairFilters.value.forall { c =>
              list.exists(v => v.annotations.find(_._1 == c._1).get._2.forall(c._2))
            }
        }
        }
    }

    val result = sc.union(futures2).collect()

    println(Await.result(Future.sequence(futures2.map(_.countAsync().asInstanceOf[Future[Long]])), Duration.Inf).sum)

    Thread.sleep(1000000)
    sc.stop()

    logger.info("Done")
  }

  def toFarAway(list1: List[Region],
                list2: List[Region],
                maxDistance: Option[Long]): Boolean = {
    maxDistance match {
      case Some(distance) =>
        list1.exists(r1 => list2.exists(r2 => r1.contig == r2.contig && r1.distance(r2).get <= distance))
      case _ => true
    }
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
          }
        }
        val alleles: Array[String] = if (refAlleles.length > 1) {
          throw new UnsupportedOperationException("Multiple reference alleles found")
        } else allAlleles.map(_.toString).toArray
        val genotypes = samples.value.map { sampleId =>
          val bla = records.flatMap(x => Option(x.getGenotype(sampleId)))
          val g = bla.head.getAlleles.map(x => alleles.indexOf(x.toString).toShort).toArray
          Genotype(g.toList, bla.head.getDP, bla.head.getDP)
        }
        Variant(bedRecord.chr, position, alleles.toList, genotypes.toList, annotations.toList)
      }
    }
  }

  /** creates regions to analyse */
  def generateRegions(cmdArgs: Args): List[List[Region]] = {
    val regions = (cmdArgs.regions match {
      case Some(i) =>
        BedRecordList.fromFile(i).validateContigs(cmdArgs.reference)
      case _ => BedRecordList.fromReference(cmdArgs.reference)
    }).combineOverlap
      .scatter(cmdArgs.binSize,
        maxContigsInSingleJob = Some(cmdArgs.maxContigsInSingleJob))
    regions.map(x => x.map(y => Region(y.chr, y.start, y.end)))
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
