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

import java.io.File

case class Args(inputFiles: List[File] = Nil,
                outputDir: File = null,
                reference: File = null,
                regions: Option[File] = None,
                aggregation: Option[File] = None,
                pedFiles: List[File] = Nil,
                externalFiles: Map[String, File] = Map(),
                singleExternalFilters: List[AnnotationFilter] = Nil,
                pairExternalFilters: List[AnnotationFilter] = Nil,
                detectionMode: DetectionMode.Value = DetectionMode.Varant,
                singleAnnotationFilter: List[AnnotationFilter] = Nil,
                pairAnnotationFilter: List[AnnotationFilter] = Nil,
                fractions: FractionsCutoffs = FractionsCutoffs(),
                usingOtherFamilies: Boolean = false,
                maxDistance: Option[Long] = None,
                binSize: Int = 1000000,
                maxContigsInSingleJob: Int = 250,
                sparkMaster: Option[String] = None,
                onlyFamily: Option[String] = None)
