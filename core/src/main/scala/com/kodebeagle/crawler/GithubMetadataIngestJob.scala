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

package com.kodebeagle.crawler

import java.net.URI

import com.kodebeagle.configuration.KodeBeagleConfig
import com.kodebeagle.crawler.GithubApiHelper._
import com.kodebeagle.util.Conversions._
import com.kodebeagle.util.SparkIndexJobHelper._
import com.kodebeagle.util.Utils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SerializableWritable, SparkConf}

object GithubMetadataIngestJob {

  def main(args: Array[String]): Unit = {
    val outputPath = KodeBeagleConfig.repoMetaDataHdfsPath

    val conf: SparkConf = new SparkConf()
    conf setAppName "GithubMetadataIngest"
    conf setMaster KodeBeagleConfig.sparkMaster
    conf set("spark.executor.memory", "10g")

    val sc = createSparkContext(conf)
    val hadoopConf = sc.hadoopConfiguration
    hadoopConf.set("dfs.replication", "1")

    val bcConf = sc.broadcast(new SerializableWritable(hadoopConf))

    val range = findRangeToIngest(outputPath, hadoopConf)

    if (range.nonEmpty) {
      val numOfSlices = range.size
      sc parallelize(range, numOfSlices) map listRepoMetadata foreach
        (Utils.write(outputPath, _, bcConf.value.value))
    } else {
      log warn
        """Repository metadata in this range is already ingested.
          | Stopping SparkContext""".stripMargin
      sc.stop()
    }
  }

  private def listRepoMetadata(seq: IndexedSeq[Int]): (Int, Int, String) = {
    val repoNames = queryRepoNamesWithInRange(seq.head, seq.last)
    val listOfRepoMetadata = repoNames.flatMap(queryRepoMetadata).mkString("\n")
    (seq.head, seq.last, listOfRepoMetadata)
  }

  private def findRangeToIngest(outputPath: String, conf: Configuration): List[IndexedSeq[Int]] = {
    val rangeArr = KodeBeagleConfig.metadataRange.split("-").map(_.trim.toInt)
    val start = rangeArr.head
    val end = rangeArr.last
    val chunkSize = KodeBeagleConfig.chunkSize.toInt

    val completeRange = (start until end).grouped(chunkSize).toList
    val rangesIngested = getRangesIngested(start, end, outputPath, conf)
    completeRange.diff(rangesIngested)
  }

  private def getRangesIngested(start: Int, end: Int,
                                outputPath: String, conf: Configuration): List[Range.Inclusive] = {
    val fs = FileSystem.get(URI.create(outputPath), conf)
    val files = fs.listFiles(new Path(outputPath), false).map(_.getPath.getName).toList
    files flatMap { file =>
      val rangeArr = file.split("-")
      val fStart = rangeArr.head.toInt
      val fEnd = rangeArr.last.toInt

      if (start <= fStart && fEnd <= end) {
        Some(fStart to fEnd)
      } else {
        None
      }
    }
  }
}


