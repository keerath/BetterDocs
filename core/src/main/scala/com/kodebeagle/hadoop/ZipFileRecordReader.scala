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
import java.util.zip.ZipInputStream

import com.kodebeagle.crawler.ZipBasicParser
import com.kodebeagle.indexer.Repository
import com.kodebeagle.parser.RepoFileNameParser
import com.kodebeagle.spark.Input
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}

/**
  * This RecordReader implementation extracts individual files from a ZIP
  * file and hands them over to the Mapper. The "key" is the decompressed
  * file name, the "value" is the file contents.
  */
class ZipFileRecordReader extends RecordReader[Option[Repository], Input] {
  /**
    * InputStream used to read the ZIP file from the FileSystem
    */
  private var fsin: FSDataInputStream = _
  /**
    * ZIP file parser/decompresser
    */
  private var zip: ZipInputStream = _
  /**
    * Uncompressed file name
    */
  private var currentKey: Option[Repository] = _
  /**
    * Uncompressed file contents
    */
  private var currentValue: Input = _
  /**
    * Used to indicate progress
    */
  private var isFinished: Boolean = false
  private var path: Path = _
  private var hasNext: Boolean = true

  /**
    * Initialise and open the ZIP file from the FileSystem
    */
  @throws(classOf[IOException])
  @throws(classOf[InterruptedException])
  override def initialize(inputSplit: InputSplit, taskAttemptContext: TaskAttemptContext) {
    val split: FileSplit = inputSplit.asInstanceOf[FileSplit]
    val conf: Configuration = taskAttemptContext.getConfiguration
    path = split.getPath
    val fs: FileSystem = path.getFileSystem(conf)
    fsin = fs.open(path)
    zip = new ZipInputStream(fsin)
  }

  /**
    * This is where the magic happens, each ZipEntry is decompressed and
    * readied for the Mapper. The contents of each file is held *in memory*
    * in a BytesWritable object.
    * <p/>
    * If the ZipFileInputFormat has been set to Lenient (not the default),
    * certain exceptions will be gracefully ignored to prevent a larger job
    * from failing.
    */

  override def nextKeyValue: Boolean = {
    if (hasNext) {
      val mayBeRepoInfo = RepoFileNameParser(path.getName)
      val parse = ZipBasicParser.readFilesAndPackages(mayBeRepoInfo, zip) // parses all zip entries
      currentKey = parse._4
      currentValue = Input(parse._1, parse._2, parse._4, parse._3)
      isFinished = true
      hasNext = false // all zip entries parsed, so nextKeyValue called once for each zip
      true
    } else {
      false
    }
  }

  /**
    * Rather than calculating progress, we just keep it simple
    */
  @throws(classOf[IOException])
  @throws(classOf[InterruptedException])
  override def getProgress: Float = {
    if (isFinished) 1 else 0
  }

  /**
    * Returns the current key (name of the zipped file)
    */
  @throws(classOf[IOException])
  @throws(classOf[InterruptedException])
  override def getCurrentKey: Option[Repository] = {
    currentKey
  }

  /**
    * Returns the current value (contents of the zipped file)
    */
  @throws(classOf[IOException])
  @throws(classOf[InterruptedException])
  override def getCurrentValue: Input = {
    currentValue
  }

  /**
    * Close quietly, ignoring any exceptions
    */
  @throws(classOf[IOException])
  override def close {
    try {
      zip.close
    }
    catch {
      case ignore: Exception => {
      }
    }
    try {
      fsin.close
    }
    catch {
      case ignore: Exception => {
      }
    }
  }
}
