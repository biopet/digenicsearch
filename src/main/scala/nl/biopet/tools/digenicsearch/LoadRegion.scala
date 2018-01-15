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
import org.apache.spark.broadcast.Broadcast

import scala.collection.JavaConversions._

/**
  * This method will return a iterator of [[Variant]]
  *
  * @param inputReaders Vcf input readers
  * @param region Single regions to load
  * @param samples Samples ID's that should be found
  * @param annotationsFields Info fields that need to be kept, the rest is not loaden into memory
  * @return
  */
class LoadRegion(inputReaders: List[VCFFileReader],
                 region: Region,
                 samples: Broadcast[Array[String]],
                 annotationsFields: Broadcast[Set[String]])
    extends Iterator[Variant] {
  protected val iterators: List[BufferedIterator[VariantContext]] =
    inputReaders
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
    val annotations = annotationsFields.value.map { field =>
      AnnotationValue(field, records.flatMap { record =>
        if (record.hasAttribute(field))
          Some(record.getAttributeAsDouble(field, 0.0))
        else None
      })
    }
    val allAllelesString: Array[String] = if (refAlleles.length > 1) {
      throw new UnsupportedOperationException(
        "Multiple reference alleles found")
    } else allAlleles.map(_.toString).toArray
    val genotypes = samples.value.map { sampleId =>
      val genotypes = records.flatMap(x => Option(x.getGenotype(sampleId)))
      val (genotype, alleles) = genotypes.headOption match {
        case _ if genotypes.length > 1 =>
          throw new IllegalStateException(
            s"Sample '$sampleId' found in mutliple times in: $records")
        case Some(x) =>
          (x,
           x.getAlleles
             .map(x => allAllelesString.indexOf(x.toString).toShort))
        case _ =>
          throw new IllegalStateException(
            s"Sample '$sampleId' not found in $records")
      }
      Genotype(alleles.toList, genotype.getDP, genotype.getDP)
    }
    Variant(region.contig,
            position,
            allAllelesString.toList,
            genotypes.toList,
            annotations.toList)
  }
}
