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
package com.kodebeagle.spark

import java.util.zip.ZipInputStream

import com.kodebeagle.configuration.KodeBeagleConfig
import com.kodebeagle.indexer.Repository
import com.kodebeagle.parser.JsParser
import com.kodebeagle.util.SparkIndexJobHelper._
import com.kodebeagle.util.{RepoFileNameParser, Utils}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object IndexJobForJS {

  private def makeZipFileExtractedRDD(sc: SparkContext):
  RDD[(List[(String, String)], Option[Repository])] = {
    val zipFileNameRDD = sc.binaryFiles(KodeBeagleConfig.githubDir).map { case (zipFile, stream) =>
      (zipFile.stripPrefix("file:").stripPrefix("hdfs:"), stream)
    }

    val zipFileExtractedRDD = zipFileNameRDD.map { case (zipFileName, stream) =>
      val repofileNameInfo = RepoFileNameParser(zipFileName)
      val (filesMap, repo) =
        Utils.readJSFiles(repofileNameInfo, new ZipInputStream(stream.open()))
      (filesMap, repo)
    }
    zipFileExtractedRDD
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster(KodeBeagleConfig.sparkMaster)
      .setAppName("IndexJobForJS")

    val sc: SparkContext = createSparkContext(conf)

    val zipFileExtractedRDD: RDD[(List[(String, String)], Option[Repository])] =
      makeZipFileExtractedRDD(sc)

    zipFileExtractedRDD.map { entry =>
      val (jsFiles, repo) = entry
      (repo, JsParser.generateTypeReferences(jsFiles.toMap, repo),
        jsFiles.map(mapToSourceFile(repo, _)).toSet)
    }.flatMap {
      case (Some(repository), jsIndices, sourceFiles) =>
        Seq(toIndexTypeJson("typereference", "jsexternal", jsIndices, isToken = false),
          toJson(repository, isToken = false), toJson(sourceFiles, isToken = false))
      case _ => Seq()
    }.saveAsTextFile(KodeBeagleConfig.sparkIndexOutput)
  }
}
