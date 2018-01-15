/*
 * Copyright (c) 2017 Sequencing Analysis Support Core - Leiden University Medical Center
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package nl.biopet.tools.digenicsearch

import java.io.{File, PrintWriter}

import htsjdk.variant.variantcontext.VariantContext
import htsjdk.variant.vcf.VCFFileReader
import nl.biopet.utils.ngs.intervals.{BedRecord, BedRecordList}
import nl.biopet.utils.ngs.vcf
import nl.biopet.utils.tool.ToolCommand
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.collection.JavaConversions._
import scala.collection.immutable
import scala.concurrent.ExecutionContext.Implicits.global

object DigenicSearch extends ToolCommand[Args] {
  def emptyArgs = Args()
  def argsParser = new ArgsParser(this)

  def main(args: Array[String]): Unit = {
    val cmdArgs = cmdArrayToArgs(args)

    logger.info("Start")
    val sparkConf: SparkConf =
      new SparkConf(true).setMaster(cmdArgs.sparkMaster.getOrElse("local[1]"))
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    implicit val sc: SparkContext = sparkSession.sparkContext
    println(s"Context is up, see ${sc.uiWebUrl.getOrElse("")}")

    val samples =
      sc.broadcast(cmdArgs.inputFiles.flatMap(vcf.getSampleIds).toArray)
    require(samples.value.lengthCompare(samples.value.distinct.length) == 0,
            "Duplicated samples detected")

    val annotations: Broadcast[Set[String]] = sc.broadcast(
      cmdArgs.singleAnnotationFilter
        .map(_.key)
        .toSet ++ cmdArgs.pairAnnotationFilter.map(_.key).toSet)

    val maxDistance = sc.broadcast(cmdArgs.maxDistance)
    val singleFilters: Broadcast[List[AnnotationFilter]] =
      sc.broadcast(cmdArgs.singleAnnotationFilter)
    val pairFilters: Broadcast[List[AnnotationFilter]] =
      sc.broadcast(cmdArgs.pairAnnotationFilter)
    val inputFiles = sc.broadcast(cmdArgs.inputFiles)
    val regions: Array[List[Region]] = generateRegions(cmdArgs).toArray
    val regionsRdds: Map[Int, RDD[List[Variant]]] =
      loadRegions(regions, inputFiles, samples, annotations).map {
        case (id, rdd) =>
          (id, rdd.map(_.filter(singleFilter(_, singleFilters))).cache())
      }

    val combinations = createCombinations(regionsRdds, regions, maxDistance)
      .map(distanceFilter(_, maxDistance))
      .map(pairedFilter(_, pairFilters))

    val outputFile = new File(cmdArgs.outputDir, "pairs.tsv")
    writeOutput(sc.union(combinations), outputFile)

    sc.stop()
    logger.info("Done")
  }

  /** This filters if 1 of the 2 variants returns true */
  def pairedFilter(v1: Variant,
                   v2: Variant,
                   pairFilters: List[AnnotationFilter]): Boolean = {
    val list = List(v1, v2)
    pairFilters.isEmpty ||
    pairFilters.forall { c =>
      list.exists(
        v =>
          v.annotations
            .find(_.key == c.key)
            .toList
            .flatMap(_.value)
            .forall(c.method))
    }
  }

  /** This filters if 1 of the 2 variants returns true */
  def pairedFilter(rdd: RDD[(Variant, Variant)],
                   pairFilters: Broadcast[List[AnnotationFilter]])
    : RDD[(Variant, Variant)] = {
    rdd.filter { case (v1, v2) => pairedFilter(v1, v2, pairFilters.value) }
  }

  /** This created a list of combinations rdds */
  def createCombinations(regionsRdds: Map[Int, RDD[List[Variant]]],
                         regions: Array[List[Region]],
                         maxDistance: Broadcast[Option[Long]])
    : IndexedSeq[RDD[(Variant, Variant)]] =
    for (i <- regions.indices; j <- i until regions.length
         if distanceFilter(regions(i), regions(j), maxDistance.value)) yield {
      val same = i == j
      val rdd = regionsRdds(i)
        .union(regionsRdds(j))
        .repartition(1)
      rdd.mapPartitions { records =>
        val list1 = records.next()
        val list2 = records.next()
        for {
          (v1, id1) <- list1.zipWithIndex.toIterator
          (v2, id2) <- list2.zipWithIndex
          if !same || id1 < id2
        } yield {
          val sorted = List(v1, v2).sortBy(v => (v.contig, v.pos))
          (sorted(0), sorted(1))
        }
      }
    }

  /** Write output to a single file */
  def writeOutput(rdd: RDD[(Variant, Variant)], outputFile: File): Unit = {
    val writer = new PrintWriter(outputFile)
    rdd
      .map {
        case (v1, v2) =>
          (v1.contig, v1.pos, v2.contig, v2.pos)
      }
      .toLocalIterator
      .foreach {
        case (c1, p1, c2, p2) => writer.println(s"$c1\t$p1\t$c2\t$p2")
      }
    writer.close()
  }

  /** This filters region combinations when maxDistance is set */
  def distanceFilter(list1: List[Region],
                     list2: List[Region],
                     maxDistance: Option[Long]): Boolean = {
    maxDistance match {
      case Some(distance) =>
        list1.exists(r1 =>
          list2.exists(r2 =>
            r1.distance(r2) match {
              case Some(x) => x <= distance
              case _ => false
          }))
      case _ => true
    }
  }

  /** This returns false is the variants are to far a way */
  def distanceFilter(v1: Variant,
                     v2: Variant,
                     maxDistance: Option[Long]): Boolean = {
    maxDistance match {
      case Some(_) if v1.contig != v2.contig => false
      case Some(distance) => (v1.pos - v2.pos).abs <= distance
      case _ => true
    }
  }

  /** This filters when maxDistance is set */
  def distanceFilter(
      rdd: RDD[(Variant, Variant)],
      maxDistance: Broadcast[Option[Long]]): RDD[(Variant, Variant)] = {
    rdd.filter { case (v1, v2) => distanceFilter(v1, v2, maxDistance.value) }
  }

  /** This filters by looking only at a single variants,
    * this reduces the number of combinations, this step is only there to improve performance */
  def singleFilter(
      v: Variant,
      singleFilters: Broadcast[List[AnnotationFilter]]): Boolean = {
    singleFilters.value.isEmpty ||
    singleFilters.value.forall { c =>
      v.annotations
        .find(_.key == c.key)
        .toList
        .flatMap(_.value)
        .forall(c.method)
    }
  }

  /**
    * Load multiple regions as a single chunk into spark
    * @param regions Regions to load
    * @param inputFiles Files to read
    * @param samples Sample ID's
    * @param annotations Info fields to read
    * @param sc Spark context to load variants
    * @return
    */
  def loadRegions(regions: Array[List[Region]],
                  inputFiles: Broadcast[List[File]],
                  samples: Broadcast[Array[String]],
                  annotations: Broadcast[Set[String]])(
      implicit sc: SparkContext): Map[Int, RDD[List[Variant]]] = {
    regions.zipWithIndex.map {
      case (r, idx) =>
        idx -> sc
          .parallelize(Seq(r), 1)
          .mapPartitions { it =>
            val readers = inputFiles.value.map(new VCFFileReader(_))
            it.map { region =>
              region
                .flatMap(
                  new LoadRegion(readers, _, samples, annotations).toList)
            }
          }
    }.toMap
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
      |This will will search for a combination of variants within a multi sample vcf file.
      |The tool can filter on INFO fields and a maximum distance of the snps on the reference.
    """.stripMargin

  def manualText: String =
    """
      |Because of the number of possible combination this tool requires to run on a spark cluster.
      |If required the tool can still run local by submitting the tool to a local master, see also https://spark.apache.org/docs/latest/submitting-applications.html#master-urls
      |By default this tool runs on the complete genome but with the option --regions a bed file can be provided to limit the number of locations
    """.stripMargin

  def exampleText: String =
    s"""
      |A default run:
      |${example("-i",
                 "<input vcf>",
                 "-o",
                 "<output dir>",
                 "-R",
                 "<reference fasta>")}
      |
      |A run on limited locations:
      |${example("-i",
                 "<input vcf>",
                 "-o",
                 "<output dir>",
                 "-R",
                 "<reference fasta>",
                 "--regions",
                 "<bed file>")}
    """.stripMargin
}
