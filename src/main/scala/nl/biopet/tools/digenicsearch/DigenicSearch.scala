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

import htsjdk.variant.vcf.VCFFileReader
import nl.biopet.utils.ngs.intervals.BedRecordList
import nl.biopet.utils.ngs.ped.PedigreeFile
import nl.biopet.utils.ngs.vcf
import nl.biopet.utils.tool.ToolCommand
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
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
    import sparkSession.implicits._
    implicit val sc: SparkContext = sparkSession.sparkContext
    println(s"Context is up, see ${sc.uiWebUrl.getOrElse("")}")

    val samples: Broadcast[Array[String]] =
      sc.broadcast(cmdArgs.inputFiles.flatMap(vcf.getSampleIds).toArray)
    require(samples.value.lengthCompare(samples.value.distinct.length) == 0,
            "Duplicated samples detected")

    val pedigree: Broadcast[PedigreeFileArray] = {
      val pedSamples =
        cmdArgs.pedFiles.map(PedigreeFile.fromFile).reduce(_ + _)
      sc.broadcast(
        PedigreeFileArray(new PedigreeFile(pedSamples.samples.filter {
          case (s, _) =>
            samples.value.contains(s)
        }), samples.value))
    }

    samples.value.foreach(
      id =>
        require(pedigree.value.pedFile.samples.contains(id),
                s"Sample '$id' not found in ped files"))

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
    val fractionsCutoffs = sc.broadcast(cmdArgs.fractions)

    logger.info("Broadcasting done")

    val regions: Broadcast[Array[List[Region]]] =
      sc.broadcast(generateRegions(cmdArgs).toArray)

    val regionsRdd: RDD[(Int, List[Region])] =
      sc.parallelize(regions.value.zipWithIndex.map {
        case (list, idx) => idx -> list
      }, regions.value.length)

    logger.info("Regions generated")

    val regionsRdds = regionsRdd
      .map {
        case (idx, r) =>
          loadRegions(idx, r, inputFiles, samples, annotations)
      }
      .map { v =>
        v.copy(variants =
          v.variants.filter(singleAnnotationFilter(_, singleFilters)))
      }
      .map { v =>
        v.copy(variants =
          v.variants.map(FractionsCutoffs.getFraction(_, pedigree.value)))
      }
      .map { v =>
        v.copy(variants = v.variants.filter(
          fractionsCutoffs.value.singleFractionFilter(_, pedigree.value)))
      }
      .toDS()
      .cache()

    println(
      "Total variants: " + regionsRdds
        .map(_.variants.length.toLong)
        .collect()
        .sum)

    logger.info("Rdd loading done")

    val indexCombination = createCombinations(regionsRdd, regions, maxDistance)

    val indexCombinationDs = indexCombination.toDS() //.repartition(indexCombination.count.toInt)

    logger.info("Combination regions done")

    val singleCombination = indexCombinationDs
      .joinWith(regionsRdds, indexCombinationDs("i1") === regionsRdds("idx"))
      .map { case (c, v) => CombinationSingle(v, c.i2) }
    val combination = singleCombination
      .joinWith(regionsRdds, singleCombination("i2") === regionsRdds("idx"))
      .map { case (c, v) => CombinationVariantList(c.i1, v) }

    val variantCombinations = combination.flatMap { x =>
      val same = x.i1.idx == x.i2.idx
      var count = 0L
      for {
        (v1, id1) <- x.i1.variants.zipWithIndex.toIterator
        (v2, id2) <- x.i2.variants.zipWithIndex
        if (!same || id1 < id2) && distanceFilter(v1, v2, maxDistance.value) &&
          pairedFilter(v1, v2, pairFilters.value) &&
          fractionsCutoffs.value.pairFractionFilter(v1, v2, pedigree.value)
      } yield (v1, v2)
    }

    val outputFile = new File(cmdArgs.outputDir, "pairs.tsv")
    writeOutput(variantCombinations, outputFile)

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

  /** This created a list of combinations rdds */
  def createCombinations(
      regionsRdd: RDD[(Int, List[Region])],
      broadcastRegions: Broadcast[Array[List[Region]]],
      maxDistance: Broadcast[Option[Long]]): RDD[Combination] = {
    regionsRdd.flatMap {
      case (idx, regions) =>
        for (i <- idx until broadcastRegions.value.length
             if distanceFilter(regions,
                               broadcastRegions.value(i),
                               maxDistance.value)) yield {
          Combination(idx, i)
        }
    }
  }

  /** Write output to a single file */
  def writeOutput(rdd: Dataset[(Variant, Variant)], outputFile: File): Unit = {
    val writer = new PrintWriter(outputFile)
    rdd.rdd
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

  /** This filters by looking only at a single variants,
    * this reduces the number of combinations, this step is only there to improve performance */
  def singleAnnotationFilter(
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
    * @return
    */
  def loadRegions(idx: Int,
                  regions: List[Region],
                  inputFiles: Broadcast[List[File]],
                  samples: Broadcast[Array[String]],
                  annotations: Broadcast[Set[String]]): IndexedVariantsList = {
    val readers = inputFiles.value.map(new VCFFileReader(_))
    IndexedVariantsList(
      idx,
      regions.flatMap(new LoadRegion(readers, _, samples, annotations)))
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
                 "<reference fasta>",
                 "-p",
                 "<ped file")}
      |
      |A run on limited locations:
      |${example("-i",
                 "<input vcf>",
                 "-o",
                 "<output dir>",
                 "-R",
                 "<reference fasta>",
                 "--regions",
                 "<bed file>",
                 "-p",
                 "<ped file")}
    """.stripMargin
}
