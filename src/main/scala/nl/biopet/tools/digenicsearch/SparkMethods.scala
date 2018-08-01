/*
 * Copyright (c) 2017 Biopet
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

import htsjdk.samtools.SAMSequenceDictionary
import htsjdk.variant.vcf.VCFFileReader
import nl.biopet.utils.Logging
import nl.biopet.utils.conversions.mapToYamlFile
import nl.biopet.utils.ngs.intervals.BedRecordList
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration

object SparkMethods extends Logging {
  def main(cmdArgs: Args): Unit = {
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
    val aggregateRegions =
      createAggregateRegions(cmdArgs.aggregation, broadcasts.value.dict)
    val variants = variantsDataSet(regions, broadcasts)
      .filter(x => singleAnnotationFilter(x, broadcasts.value))
      .flatMap(x => x.filterExternalFractions(broadcasts.value))
      .cache()

    val familyAggregateFuture = familyAggregation(variants,
                                                  broadcasts,
                                                  aggregateRegions,
                                                  cmdArgs.outputDir)
    val familyFilter = Future.sequence(
      cmdArgs.onlyFamily
        .map(x => List(broadcasts.value.pedigree.families.indexOf(x)))
        .getOrElse(broadcasts.value.pedigree.families.indices.toList)
        .map(
          id =>
            familyCombinations(variants,
                               broadcasts,
                               id,
                               regions,
                               cmdArgs.outputDir)))

    val allCombinationFutures =
      if (cmdArgs.onlyFamily.isEmpty)
        allCombination(variants, broadcasts, regions, cmdArgs.outputDir)
      else Nil

    val futures = allCombinationFutures ::: List(
      Future(
        aggregateRegions.foreach(aggregateTotal(_, variants).write
          .csv(outputAggregation(cmdArgs.outputDir).getAbsolutePath))),
      familyFilter
    ) ::: familyAggregateFuture.toList
    variants.unpersist()
    Await.result(Future.sequence(futures), Duration.Inf)

    sparkSession.stop()
    logger.info("Done")
  }

  def allCombination(variants: Dataset[Variant],
                     broadcasts: Broadcast[Broadcasts],
                     regions: Dataset[IndexedRegions],
                     outputDir: File)(
      implicit sparkSession: SparkSession): List[Future[Unit]] = {
    import sparkSession.implicits._
    val variantsAllFiltered =
      variants.flatMap(x => x.filterSingleFraction(broadcasts.value)).cache()
    val singleFilterTotal = variantsAllFiltered.rdd.countAsync()

    val combinationsAll =
      createVariantCombinations(variantsAllFiltered, regions, broadcasts)

    val combinationAllFilter =
      filterAllVariantCombinations(combinationsAll, broadcasts)
        .map(_.cleanResults)
        .map(_.toResultLine(broadcasts.value))
        .cache()

    List(
      Future(
        combinationAllFilter.write.csv(outputPairs(outputDir).getAbsolutePath)),
      Future(
        variantsAllFiltered
          .map(_.toCsv(broadcasts.value))
          .write
          .csv(outputVariants(outputDir).getAbsolutePath)),
      Future(
        writeStatsFile(outputDir,
                       Await.result(singleFilterTotal, Duration.Inf),
                       combinationAllFilter.count()))
    )
  }

  def familyCombinations(
      variants: Dataset[Variant],
      broadcasts: Broadcast[Broadcasts],
      familyId: Int,
      regions: Dataset[IndexedRegions],
      outputDir: File)(implicit sparkSession: SparkSession): Future[Unit] = {
    import sparkSession.implicits._
    val variantsFamilyFiltered =
      variants
        .flatMap(x => x.filterFamilyFractions(broadcasts.value, Some(familyId)))
        .cache()

    val combinations =
      createVariantCombinations(variantsFamilyFiltered, regions, broadcasts)

    val filterCombinations = combinations
      .filter(x => distanceFilter(x.v1, x.v2, broadcasts.value.maxDistance))
      .filter(pairedAnnotationFilter(_, broadcasts.value.pairFilters))
      .flatMap(_.filterPairFraction(broadcasts.value, Some(familyId)))
      .flatMap(_.filterExternalPair(broadcasts.value))

    Future(
      filterCombinations
        .map(_.toResultLine(broadcasts.value))
        .write
        .csv(outputFamilyPairs(
          outputDir,
          broadcasts.value.pedigree.families(familyId)).getAbsolutePath))
  }

  def familyAggregation(variants: Dataset[Variant],
                        broadcasts: Broadcast[Broadcasts],
                        aggregateRegions: Option[Dataset[Region]],
                        outputDir: File)(
      implicit sparkSession: SparkSession): Option[Future[Unit]] = {
    import sparkSession.implicits._
    val variantsAggregateFamilyFiltered =
      variants.flatMap(x => x.filterFamilyFractions(broadcasts.value)).cache()
    val aggregateFamilies = aggregateRegions.map(
      createAggregateFamilies(_, variantsAggregateFamilyFiltered, broadcasts))

    aggregateFamilies.flatMap { counts =>
      Some(counts.rdd.sortBy(_.gene).collectAsync().map { data =>
        val writer = new PrintWriter(outputFamilyGenes(outputDir))
        writer.println(
          "gene\t" + broadcasts.value.pedigree.families.mkString("\t"))
        data.foreach(x =>
          writer.println(x.gene + "\t" + x.count.mkString("\t")))
        writer.close()
      })
    }
  }

  /** Create regions as dataset */
  def regionsDataset(broadcasts: Broadcast[Broadcasts])(
      implicit sparkSession: SparkSession): Dataset[IndexedRegions] = {
    import sparkSession.implicits._
    sparkSession.sparkContext
      .parallelize(broadcasts.value.regions.zipWithIndex.map {
        case (list, idx) => IndexedRegions(idx, list)
      }, broadcasts.value.regions.length)
      .toDS()
  }

  def outputFamilyGenes(outputDir: File): File =
    new File(outputDir, "familyGenes.tsv")
  def outputAggregation(outputDir: File): File =
    new File(outputDir, "aggregation")
  def outputPairs(outputDir: File): File = new File(outputDir, "pairs")
  def outputFamilyPairs(outputDir: File, familyName: String) =
    new File(outputDir, "family" + File.separator + familyName)
  def outputVariants(outputDir: File): File = new File(outputDir, "variants")

  /** Create aggregate regions as dataset if this is given on the commandline */
  def createAggregateRegions(aggregationFile: Option[File],
                             dict: SAMSequenceDictionary)(
      implicit sparkSession: SparkSession): Option[Dataset[Region]] = {
    import sparkSession.implicits._
    aggregationFile.map { file =>
      sparkSession.sparkContext
        .parallelize(Region.fromBedFile(file, dict))
        .toDS()
        .cache()
    }
  }

  /** Aggregate on families and genes (or other features inside the bed file) */
  def createAggregateFamilies(aggregateRegion: Dataset[Region],
                              variants: Dataset[Variant],
                              broadcasts: Broadcast[Broadcasts])(
      implicit sparkSession: SparkSession): Dataset[GeneFamilyCounts] = {
    import sparkSession.implicits._
    aggregateRegion
      .joinWith(
        variants,
        aggregateRegion("contig") === variants("contig") && aggregateRegion(
          "start") <= variants("pos") && aggregateRegion("end") >= variants(
          "pos"))
      .rdd
      .map {
        case (r: Region, v) => r.name -> v.passedFamilies(broadcasts.value)
      }
      .groupByKey()
      .map {
        case (gene, passed) =>
          // This counting gene per family into a Array[Int]
          val start = Array.fill(broadcasts.value.pedigree.families.length)(0L)
          GeneFamilyCounts(
            gene,
            passed.foldLeft(start) {
              case (result, families) =>
                result.zip(families).map {
                  case (counts, bool) => if (bool) counts + 1 else counts
                }
            }
          )
      }
      .toDS()
  }

  /** Aggregate on genes (or other features inside the bed file) */
  def aggregateTotal(aggregateRegion: Dataset[Region],
                     variants: Dataset[Variant])(
      implicit sparkSession: SparkSession): Dataset[GeneCounts] = {
    import sparkSession.implicits._
    aggregateRegion
      .joinWith(
        variants,
        aggregateRegion("contig") === variants("contig") && aggregateRegion(
          "start") <= variants("pos") && aggregateRegion("end") >= variants(
          "pos"))
      .rdd
      .map { case (r, v) => r.name -> v }
      .groupByKey()
      .map { case (g, l) => GeneCounts(g, l.size) }
      .toDS()
  }

  def checkExistsOutput(outputDir: File): Unit = {
    require(!outputPairs(outputDir).exists(),
            s"Output file already exists: ${outputPairs(outputDir)}")
    require(!outputVariants(outputDir).exists(),
            s"Output file already exists: ${outputVariants(outputDir)}")
  }

  /** Writing a stats file */
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

  /** Created all posible combinations of variants */
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
          if (v1.contig < v2.contig || v1.pos < v2.pos)
            Some(
              VariantCombination(v1,
                                 v2,
                                 Variant.alleleCombinations(v1, v2).toList))
          else None
      }
  }

  /** Filter combinations */
  def filterAllVariantCombinations(combinations: Dataset[VariantCombination],
                                   broadcasts: Broadcast[Broadcasts])(
      implicit sparkSession: SparkSession): Dataset[VariantCombination] = {
    import sparkSession.implicits._

    combinations
      .filter(x => distanceFilter(x.v1, x.v2, broadcasts.value.maxDistance))
      .filter(pairedAnnotationFilter(_, broadcasts.value.pairFilters))
      .flatMap(_.filterPairFraction(broadcasts.value))
      .flatMap(_.filterExternalPair(broadcasts.value))
  }

  /** This reads all regions and makes a [[Variant]] dataset */
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
  def pairedAnnotationFilter(combination: VariantCombination,
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
              case _       => false
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
      case Some(distance)                    => (v1.pos - v2.pos).abs <= distance
      case _                                 => true
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

}
