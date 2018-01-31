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

import htsjdk.samtools.SAMSequenceDictionary
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
    logger.info(
      s"Context is up, see ${sparkSession.sparkContext.uiWebUrl.getOrElse("")}")

    val broadcasts =
      sparkSession.sparkContext.broadcast(Broadcasts.fromArgs(cmdArgs))
    logger.info("Broadcasting done")

    val regions = regionsDataset(broadcasts)
    val variants = variantsDataSet(regions, broadcasts)
      .filter(x => singleAnnotationFilter(x, broadcasts.value))
      .flatMap(x => x.filterExternalFractions(broadcasts.value))
      .cache()
    val variantsAllFiltered =
      variants.flatMap(x => x.filterSingleFraction(broadcasts.value)).cache()
    val singleFilterTotal = variantsAllFiltered.rdd.countAsync()

    val combinations =
      createVariantCombinations(variantsAllFiltered, regions, broadcasts)

    val aggregateFuture = aggregate(cmdArgs, variants, broadcasts.value.dict)

    val combinationFilter =
      filterVariantCombinations(combinations, broadcasts)
        .map(_.cleanResults)
        .map(_.toResultLine(broadcasts.value))
        .cache()

    combinationFilter.write.csv(outputPairs(cmdArgs.outputDir).getAbsolutePath)
    writeStatsFile(cmdArgs.outputDir,
                   Await.result(singleFilterTotal, Duration.Inf),
                   combinationFilter.count())
    Await.result(aggregateFuture, Duration.Inf)

    sparkSession.stop()
    logger.info("Done")
  }

  def regionsDataset(broadcasts: Broadcast[Broadcasts])(
      implicit sparkSession: SparkSession): Dataset[IndexedRegions] = {
    import sparkSession.implicits._
    sparkSession.sparkContext
      .parallelize(broadcasts.value.regions.zipWithIndex.map {
        case (list, idx) => IndexedRegions(idx, list)
      }, broadcasts.value.regions.length)
      .toDS()
  }

  def outputPairs(outputDir: File): File = new File(outputDir, "pairs")
  def outputVariants(outputDir: File): File = new File(outputDir, "variants")

  def aggregate(cmdArgs: Args,
                variants: Dataset[Variant],
                dict: SAMSequenceDictionary)(
      implicit sparkSession: SparkSession): Future[Unit] = {
    import sparkSession.implicits._
    Future {
      cmdArgs.aggregation.foreach { file =>
        val bedrecords = sparkSession.sparkContext
          .parallelize(Region.fromBedFile(file, dict))
          .toDS()

        val j =
          bedrecords
            .joinWith(
              variants,
              bedrecords("contig") === variants("contig") && bedrecords(
                "start") <= variants("pos") && bedrecords("end") >= variants(
                "pos"))
            .rdd
            .map { case (r, v) => r.name -> v }
            .groupByKey()
            .map { case (g, l) => GeneCounts(g, l.size) }
            .toDS()

        val bla = variants.collect()
        val bla2 = bedrecords.collect()
        val bla3 = j.collect()

        val outputFile = new File(cmdArgs.outputDir, "aggregation")
        j.write.csv(outputFile.getAbsolutePath)
      }
    }
  }

  def checkExistsOutput(outputDir: File): Unit = {
    require(!outputPairs(outputDir).exists(),
            s"Output file already exists: ${outputPairs(outputDir)}")
    require(!outputVariants(outputDir).exists(),
            s"Output file already exists: ${outputVariants(outputDir)}")
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

  case class Temp(v1: Variant, i2: Int)
  def createVariantCombinations(variants: Dataset[Variant],
                                regionsRdd: Dataset[IndexedRegions],
                                broadcasts: Broadcast[Broadcasts])(
      implicit sparkSession: SparkSession): Dataset[VariantCombination] = {
    import sparkSession.implicits._

    val indexCombination = createCombinations(regionsRdd, broadcasts)

    val single = indexCombination
      .joinWith(variants, variants("regionsIdx") === indexCombination("i1"))
      .map {
        case (c, v1) =>
          Temp(v1, c.i2)
      }
    single
      .joinWith(variants, variants("regionsIdx") === single("i2"))
      .flatMap {
        case (t, v2) =>
          val v1 = t.v1
          if (v1.contig > v2.contig || v1.pos < v2.pos)
            Some(
              VariantCombination(v1,
                                 v2,
                                 Variant.alleleCombinations(v1, v2).toList))
          else None
      }
  }

  def filterVariantCombinations(combinations: Dataset[VariantCombination],
                                broadcasts: Broadcast[Broadcasts])(
      implicit sparkSession: SparkSession): Dataset[VariantCombination] = {
    import sparkSession.implicits._

    combinations
      .filter(x => distanceFilter(x.v1, x.v2, broadcasts.value.maxDistance))
      .filter(pairedFilter(_, broadcasts.value.pairFilters))
      .flatMap(_.filterPairFraction(broadcasts.value))
      .flatMap(_.filterExternalPair(broadcasts.value))
  }

  def variantsDataSet(regionsRdd: Dataset[IndexedRegions],
                      broadcasts: Broadcast[Broadcasts])(
      implicit sparkSession: SparkSession): Dataset[Variant] = {
    import sparkSession.implicits._
    regionsRdd
      .mapPartitions { it =>
        val readers = broadcasts.value.inputFiles.map(new VCFFileReader(_))
        val externalReaders =
          broadcasts.value.externalFiles.map(new VCFFileReader(_))
        it.flatMap { r =>
          r.regions.flatMap(
            new LoadRegion(readers,
                           externalReaders,
                           _,
                           r.idx,
                           broadcasts.value))
        }
      }
  }

  /** This filters if 1 of the 2 variants returns true */
  def pairedFilter(combination: VariantCombination,
                   pairFilters: List[AnnotationFilter]): Boolean = {
    val list = List(combination.v1, combination.v2)
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
  def createCombinations(regionsRdd: Dataset[IndexedRegions],
                         broadcasts: Broadcast[Broadcasts])(
      implicit sparkSession: SparkSession): Dataset[Combination] = {
    import sparkSession.implicits._
    regionsRdd.flatMap { r =>
      for (i <- r.idx until broadcasts.value.regions.length
           if distanceFilter(r.regions,
                             broadcasts.value.regions(i),
                             broadcasts.value.maxDistance)) yield {
        Combination(r.idx, i)
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

  /** creates regions to analyse */
  def generateRegions(cmdArgs: Args,
                      dict: SAMSequenceDictionary): List[List[Region]] = {
    val regions = (cmdArgs.regions match {
      case Some(i) =>
        BedRecordList.fromFile(i).validateContigs(cmdArgs.reference)
      case _ => BedRecordList.fromReference(cmdArgs.reference)
    }).combineOverlap
      .scatter(cmdArgs.binSize,
               maxContigsInSingleJob = Some(cmdArgs.maxContigsInSingleJob))
    regions.map(x =>
      x.map(y => Region(dict.getSequenceIndex(y.chr), y.start, y.end)))
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
