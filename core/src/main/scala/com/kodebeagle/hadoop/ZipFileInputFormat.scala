package com.kodebeagle.hadoop

import java.io.IOException
import com.kodebeagle.indexer.Repository
import com.kodebeagle.spark.Input
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.mapreduce.JobContext
import org.apache.hadoop.mapreduce.RecordReader
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat

/**
  * Extends the basic FileInputFormat class provided by Apache Hadoop to accept ZIP files. It should be noted that ZIP
  * files are not 'splittable' and each ZIP file will be processed by a single Mapper.
  */

class ZipFileInputFormat extends FileInputFormat[Option[Repository], Input] {
  /**
    * ZIP files are not splitable
    */
  protected override def isSplitable(context: JobContext, filename: Path): Boolean = {
    false
  }

  /**
    * Create the ZipFileRecordReader to parse the file
    */
  @throws(classOf[IOException])
  @throws(classOf[InterruptedException])
  def createRecordReader(split: InputSplit,
                         context: TaskAttemptContext): RecordReader[Option[Repository], Input] = {
    new ZipFileRecordReader
  }
}

object ZipFileInputFormat {
  private var isLenient: Boolean = false
  /**
    *
    * @param lenient
    */
  def setLenient(lenient: Boolean) {
    isLenient = lenient
  }

  def getLenient: Boolean = {
    isLenient
  }
}
