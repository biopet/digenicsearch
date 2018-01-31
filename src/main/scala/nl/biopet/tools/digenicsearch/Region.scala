package nl.biopet.tools.digenicsearch

import java.io.File

import htsjdk.samtools.SAMSequenceDictionary
import nl.biopet.utils.ngs.intervals.BedRecordList

case class Region(contig: Int, start: Int, end: Int, name: String = "") {
  def distance(other: Region): Option[Long] = {
    if (this.contig == other.contig) {
      if (this.start > other.end) Some(this.start - other.end)
      else if (other.start > this.end) Some(other.start - this.end)
      else Some(0)
    } else None
  }
}

object Region {
  def fromBedFile(file: File, dict: SAMSequenceDictionary): List[Region] = {
    val bedRecordList = BedRecordList.fromFile(file)
    bedRecordList.allRecords
      .map(
        r =>
          Region(dict.getSequenceIndex(r.chr),
                 r.start,
                 r.end,
                 r.name.getOrElse("")))
      .toList
  }
}
