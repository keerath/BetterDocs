/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
  * Extends the basic FileInputFormat class provided by Apache Hadoop to accept ZIP files.
  * It should be noted that ZIP files are not 'splittable' and each ZIP file will
  * be processed by a single Mapper.
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
