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

import com.kodebeagle.configuration.KodeBeagleConfig
import com.kodebeagle.indexer.{ExternalTypeReference, FileMetaDataIndexer, InternalTypeReference, JavaExternalTypeRefIndexer, JavaInternalTypeRefIndexer, Repository, ScalaExternalTypeRefIndexer, ScalaInternalTypeRefIndexer}
import com.kodebeagle.javaparser.JavaASTParser
import com.kodebeagle.spark.SparkIndexJobHelper._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

case class Indices(javaInternal: Set[InternalTypeReference],
                   javaExternal: Set[ExternalTypeReference],
                   scalaInternal: Set[InternalTypeReference],
                   scalaExternal: Set[ExternalTypeReference])

case class Input(javaFiles: List[(String, String)], scalaFiles: List[(String, String)],
                 repo: Option[Repository], packages: List[String])

object ConsolidatedIndexJob {

  def main(args: Array[String]): Unit = {
    val TYPEREFS = "typereferences"
    val conf = new SparkConf()
      .set("spark.driver.memory", "6g")
      .set("spark.executor.memory", "4g")
      .set("spark.network.timeout", "1200s")

    val sc: SparkContext = new SparkContext(conf)

    val zipFileExtractedRDD: RDD[(List[(String, String)], List[(String, String)],
      Option[Repository], List[String])] = makeZipFileExtractedRDD(sc)

    val javaInternalIndexer = new JavaInternalTypeRefIndexer()
    val javaExternalIndexer = new JavaExternalTypeRefIndexer()
    val scalaInternalIndexer = new ScalaInternalTypeRefIndexer()
    val scalaExternalIndexer = new ScalaExternalTypeRefIndexer()

    implicit val indexers =
      (javaInternalIndexer, javaExternalIndexer, scalaInternalIndexer, scalaExternalIndexer)

    val broadCast = sc.broadcast(new JavaASTParser(true))

    //  Create indexes for elastic search.
    zipFileExtractedRDD.map { f =>
      val (javaFiles, scalaFiles, repo, packages) = f
      val javaScalaTypeRefs =
        generateAllIndices(Input(javaFiles, scalaFiles, repo, packages))
      val sourceFiles = mapToSourceFiles(repo, javaFiles ++ scalaFiles)
      (repo, javaScalaTypeRefs.javaInternal, javaScalaTypeRefs.javaExternal,
        javaScalaTypeRefs.scalaInternal, javaScalaTypeRefs.scalaExternal,
        FileMetaDataIndexer.generateMetaData(sourceFiles, broadCast, repo.getOrElse(Repository.invalid).tag),
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
    }.saveAsTextFile(KodeBeagleConfig.sparkIndexOutput)
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
