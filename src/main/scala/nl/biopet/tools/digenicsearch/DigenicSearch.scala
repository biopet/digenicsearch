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

import java.io.File

import htsjdk.variant.vcf.VCFFileReader
import nl.biopet.utils.ngs.intervals.BedRecordList
import nl.biopet.utils.tool.ToolCommand
import nl.biopet.utils.conversions.mapToYamlFile
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
    checkExistsOutput(cmdArgs.outputDir)

    logger.info("Start")
    val sparkConf: SparkConf =
      new SparkConf(true).setMaster(cmdArgs.sparkMaster.getOrElse("local[1]"))
    implicit val sparkSession: SparkSession =
      SparkSession.builder().config(sparkConf).getOrCreate()
    import sparkSession.implicits._
    implicit val sc: SparkContext = sparkSession.sparkContext
    logger.info(s"Context is up, see ${sc.uiWebUrl.getOrElse("")}")

    val broadcasts = sc.broadcast(Broadcasts.fromArgs(cmdArgs))
    logger.info("Broadcasting done")

    val regionsRdd: RDD[(Int, List[Region])] =
      sc.parallelize(broadcasts.value.regions.zipWithIndex.map {
        case (list, idx) => idx -> list
      }, broadcasts.value.regions.length)

    val variants: Dataset[IndexedVariantsList] =
      variantsRdd(regionsRdd, broadcasts).toDS().cache()

    val singleFilterTotal = countSingleFilterTotal(variants)

    val indexCombination: Dataset[Combination] =
      createCombinations(regionsRdd, broadcasts).toDS()

    val variantCombinations =
      createVariantCombinations(variants, indexCombination, broadcasts)
      //.repartition(500)
      //.sort("contig1", "contig2", "pos1", "pos2")
        .cache()

    variantCombinations.write.parquet(
      outputFile(cmdArgs.outputDir).getAbsolutePath)

    writeStatsFile(cmdArgs.outputDir,
                   Await.result(singleFilterTotal, Duration.Inf),
                   variantCombinations.count())

    sc.stop()
    logger.info("Done")
  }

  def outputFile(outputDir: File): File = new File(outputDir, "pairs")

  def checkExistsOutput(outputDir: File): Unit = {
    require(!outputFile(outputDir).exists(),
            s"Output file already exists: ${outputFile(outputDir)}")
  }

  def writeStatsFile(outputDir: File,
                     singleFilterTotal: Long,
                     totalPairs: Long): Unit = {
    mapToYamlFile(Map(
                    "total_pairs" -> totalPairs,
                    "single_filter_total" -> singleFilterTotal
                  ),
                  new File(outputDir, "stats.yml"))
  }

  def countSingleFilterTotal(variants: Dataset[IndexedVariantsList])(
      implicit sparkSession: SparkSession): Future[Long] = {
    import sparkSession.implicits._
    val f = Future(
      variants
        .map(_.variants.length.toLong)
        .reduce(_ + _))
    f.onSuccess {
      case x => logger.info("Total variants: " + x)
    }
    f
  }

  def createVariantCombinations(variants: Dataset[IndexedVariantsList],
                                indexCombination: Dataset[Combination],
                                broadcasts: Broadcast[Broadcasts])(
      implicit sparkSession: SparkSession): Dataset[ResultLine] = {
    import sparkSession.implicits._
    val singleCombination = indexCombination
      .joinWith(variants, indexCombination("i1") === variants("idx"))
      .map { case (c, v) => CombinationSingle(v, c.i2) }
    val combinations: Dataset[CombinationVariantList] = singleCombination
      .joinWith(variants, singleCombination("i2") === variants("idx"))
      .map { case (c, v) => CombinationVariantList(c.i1, v) }

    combinations
      .flatMap { x =>
        val same = x.i1.idx == x.i2.idx
        (for {
          (v1, id1) <- x.i1.variants.zipWithIndex.toIterator
          (v2, id2) <- x.i2.variants.zipWithIndex
          if (!same || id1 < id2) && distanceFilter(
            v1,
            v2,
            broadcasts.value.maxDistance) &&
            pairedFilter(v1, v2, broadcasts.value.pairFilters) &&
            Variant
              .filterPairFraction(v1,
                                  v2,
                                  broadcasts.value.pedigree,
                                  broadcasts.value.fractionsCutoffs)
              .isDefined
        } yield {
          Variant
            .filterPairFraction(v1,
                                v2,
                                broadcasts.value.pedigree,
                                broadcasts.value.fractionsCutoffs)
            .map {
              case (c1, c2) => ResultLine(c1.contig, c1.pos, c2.contig, c2.pos)
            }
        }).flatten
      }
  }

  def variantsRdd(
      regionsRdd: RDD[(Int, List[Region])],
      broadcasts: Broadcast[Broadcasts]): RDD[IndexedVariantsList] = {
    regionsRdd
      .map {
        case (idx, r) =>
          loadRegions(idx, r, broadcasts.value)
      }
      .map { v =>
        v.copy(
          variants =
            v.variants.filter(singleAnnotationFilter(_, broadcasts.value)))
      }
      .map { v =>
        v.copy(
          variants =
            v.variants.flatMap(_.filterSingleFraction(broadcasts.value)))
      }
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
      broadcasts: Broadcast[Broadcasts]): RDD[Combination] = {
    regionsRdd.flatMap {
      case (idx, regions) =>
        for (i <- idx until broadcasts.value.regions.length
             if distanceFilter(regions,
                               broadcasts.value.regions(i),
                               broadcasts.value.maxDistance)) yield {
          Combination(idx, i)
        }
    }
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
  def singleAnnotationFilter(v: Variant, broadcasts: Broadcasts): Boolean = {
    broadcasts.singleFilters.isEmpty ||
    broadcasts.singleFilters.forall { c =>
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
    * @param broadcasts Broadcasted values
    * @return
    */
  def loadRegions(idx: Int,
                  regions: List[Region],
                  broadcasts: Broadcasts): IndexedVariantsList = {
    val readers = broadcasts.inputFiles.map(new VCFFileReader(_))
    IndexedVariantsList(
      idx,
      regions.flatMap(new LoadRegion(readers, _, broadcasts)))
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
