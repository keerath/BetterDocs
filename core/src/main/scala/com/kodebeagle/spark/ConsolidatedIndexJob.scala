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

import java.net.URI

import com.kodebeagle.configuration.KodeBeagleConfig
import com.kodebeagle.indexer.{ExternalTypeReference, FileMetaDataIndexer, InternalTypeReference, JavaExternalTypeRefIndexer, JavaInternalTypeRefIndexer, Repository, ScalaExternalTypeRefIndexer, ScalaInternalTypeRefIndexer}
import com.kodebeagle.javaparser.JavaASTParser
import com.kodebeagle.logging.Logger
import com.kodebeagle.spark.SparkIndexJobHelper._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{JobSucceeded, SparkListenerJobEnd, SparkListenerApplicationEnd, SparkListener}
import org.apache.spark.{SparkConf, SparkContext}
import scala.sys.process._

case class Indices(javaInternal: Set[InternalTypeReference],
                   javaExternal: Set[ExternalTypeReference],
                   scalaInternal: Set[InternalTypeReference],
                   scalaExternal: Set[ExternalTypeReference])

case class Input(javaFiles: List[(String, String)], scalaFiles: List[(String, String)],
                 repo: Option[Repository], packages: List[String])

object ConsolidatedIndexJob extends Logger {

  def main(args: Array[String]): Unit = {
    val TYPEREFS = "typereferences"
    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    sc.addSparkListener(new SparkListener() {
      override def onJobEnd(jobEnd: SparkListenerJobEnd) = {
        if (jobEnd.jobResult == JobSucceeded) {
          val conf = new Configuration()
          conf.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
          conf.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)
          val hdfs = FileSystem.get(URI.create("hdfs://192.168.2.145:9000"), conf)
          hdfs.delete(new Path(s"/user/zips${args(0)}"), true)
          /*s"hdfs dfs -rmr hdfs://192.168.2.145:9000/user/zips${args(0)}".!*/
          log.info(s"Processing for batch ${args(0)} completed!")
        }
      }
    })

    val zipFileExtractedRDD: RDD[(List[(String, String)], List[(String, String)],
      Option[Repository], List[String])] = makeZipFileExtractedRDD(sc, args(0))

    val javaInternalIndexer = new JavaInternalTypeRefIndexer()
    val javaExternalIndexer = new JavaExternalTypeRefIndexer()
    val scalaInternalIndexer = new ScalaInternalTypeRefIndexer()
    val scalaExternalIndexer = new ScalaExternalTypeRefIndexer()

    implicit val indexers =
      (javaInternalIndexer, javaExternalIndexer, scalaInternalIndexer, scalaExternalIndexer)

    //  Create indexes for elastic search.
    zipFileExtractedRDD.map { f =>
      val (javaFiles, scalaFiles, repo, packages) = f
      val javaScalaTypeRefs =
        generateAllIndices(Input(javaFiles, scalaFiles, repo, packages))
      val sourceFiles = mapToSourceFiles(repo, javaFiles ++ scalaFiles)
      (repo, javaScalaTypeRefs.javaInternal, javaScalaTypeRefs.javaExternal,
        javaScalaTypeRefs.scalaInternal, javaScalaTypeRefs.scalaExternal,
        FileMetaDataIndexer.generateMetaData(sourceFiles, packages,
          repo.getOrElse(Repository.invalid).tag),
        sourceFiles)
    }.flatMap { case (Some(repository), javaInternalIndices, javaExternalIndices,
    scalaInternalIndices, scalaExternalIndices, metadataIndices, sourceFiles) =>
      Seq(toJson(repository, isToken = false),
        toIndexTypeJson(TYPEREFS, "javainternal", javaInternalIndices, isToken = false),
        toIndexTypeJson(TYPEREFS, "javaexternal", javaExternalIndices, isToken = false),
        toIndexTypeJson(TYPEREFS, "scalainternal", scalaInternalIndices, isToken = false),
        toIndexTypeJson(TYPEREFS, "scalaexternal", scalaExternalIndices, isToken = false),
        toJson(metadataIndices, isToken = false), toJson(sourceFiles, isToken = false))
    case _ => Seq()
    }.saveAsTextFile(KodeBeagleConfig.sparkIndexOutput + args(0))
  }

  def generateAllIndices(jsInput: Input)
                        (implicit indexers: (JavaInternalTypeRefIndexer,
                          JavaExternalTypeRefIndexer, ScalaInternalTypeRefIndexer,
                          ScalaExternalTypeRefIndexer)): Indices = {
    val javaFiles = jsInput.javaFiles
    val scalaFiles = jsInput.scalaFiles
    val packages = jsInput.packages
    val repo = jsInput.repo
    val javaInternal = indexers._1
      .generateTypeReferences(javaFiles.toMap, packages, repo)
      .asInstanceOf[Set[InternalTypeReference]]
    val javaExternal = indexers._2
      .generateTypeReferences(javaFiles.toMap, packages, repo)
      .asInstanceOf[Set[ExternalTypeReference]]
    val scalaInternal = indexers._3
      .generateTypeReferences(scalaFiles.toMap, packages, repo)
      .asInstanceOf[Set[InternalTypeReference]]
    val scalaExternal = indexers._4
      .generateTypeReferences(scalaFiles.toMap, packages, repo)
      .asInstanceOf[Set[ExternalTypeReference]]
    Indices(javaInternal, javaExternal, scalaInternal, scalaExternal)
  }
}
