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

import htsjdk.variant.variantcontext.VariantContext
import htsjdk.variant.vcf.VCFFileReader
import nl.biopet.utils.ngs.intervals.BedRecord
import nl.biopet.utils.ngs.vcf

import scala.collection.JavaConversions._

/**
  * This method will return a iterator of [[Variant]]
  *
  * @param inputReaders Vcf input readers
  * @param externalInputReaders Vcf input readers
  * @param region Single regions to load
  * @param broadcasts Broadcast values
  * @return
  */
class LoadRegion(inputReaders: List[VCFFileReader],
                 externalInputReaders: Array[VCFFileReader],
                 region: Region,
                 broadcasts: Broadcasts)
    extends Iterator[Variant] {
  protected val iterators: List[BufferedIterator[VariantContext]] =
    inputReaders
      .map(
        vcf.loadRegion(_, BedRecord(region.contig, region.start, region.end)))
      .map(_.buffered)

  protected val externalIterators: Array[BufferedIterator[VariantContext]] =
    externalInputReaders
      .map(
        vcf.loadRegion(_, BedRecord(region.contig, region.start, region.end)))
      .map(_.buffered)

  def hasNext: Boolean = iterators.exists(_.hasNext)

  def next(): Variant = {
    val position = iterators.filter(_.hasNext).map(_.head.getStart).min
    val records = iterators
      .filter(_.hasNext)
      .filter(_.head.getStart == position)
      .map(_.next())
    val allAlleles = records.flatMap(_.getAlleles)
    val refAlleles = allAlleles.filter(_.isReference)
    val altAlleles = allAlleles.filter(!_.isReference)
    val annotations = broadcasts.annotations.map { field =>
      AnnotationValue(field, records.flatMap { record =>
        if (record.hasAttribute(field))
          Some(record.getAttributeAsDouble(field, 0.0))
        else None
      })
    }
    val allAllelesString: Array[String] = if (refAlleles.length > 1) {
      throw new UnsupportedOperationException(
        "Multiple reference alleles found")
    } else (refAlleles ::: altAlleles).map(_.toString).toArray
    val genotypes = broadcasts.samples.map { sampleId =>
      val genotypes = records.flatMap(x => Option(x.getGenotype(sampleId)))
      val (genotype, alleles) = genotypes.headOption match {
        case _ if genotypes.length > 1 =>
          throw new IllegalStateException(
            s"Sample '$sampleId' found in multiple times in: $records")
        case Some(x) =>
          (x,
           x.getAlleles
             .map(x => allAllelesString.indexOf(x.toString).toShort))
        case _ =>
          throw new IllegalStateException(
            s"Sample '$sampleId' not found in $records")
      }
      (Genotype(alleles.toList),
       GenotypeAnnotation(genotype.getDP, genotype.getDP))
    }
    val genotypes1 = genotypes.map { case (g, _) => g }.toList

    val externalGenotypes: Array[List[Genotype]] = externalIterators.map {
      it =>
        if (it.hasNext) {
          while (it.hasNext && it.head.getStart < position) it.next()
          if (it.head.getStart == position) {
            val record = it.next()
            if (record.getReference == refAlleles.head)
              (for (g <- record.getGenotypes) yield {
                Genotype(
                  g.getAlleles
                    .map(a =>
                      allAllelesString.indexOf(a.getBaseString).toShort)
                    .toList)
              }).toList
            else Nil
          } else Nil
        } else Nil
    }

    Variant(
      region.contig,
      position,
      allAllelesString.toList,
      genotypes1,
      annotations.toList,
      genotypes.map { case (_, g) => g }.toList,
      DetectionMode.valueToVal(broadcasts.detectionMode).method(genotypes1),
      externalGenotypes
    )
  }
}
