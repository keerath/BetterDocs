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


package com.kodebeagle.indexer

import com.kodebeagle.indexer.JavaFileIndexerHelper._
import com.kodebeagle.logging.Logger
import com.kodebeagle.parser.MethodVisitor

import scala.collection.immutable

class JavaExternalTypeRefIndexer extends JavaTypeRefIndexer {

  protected def extractTokensASTParser(excludePackages: Set[String],
                                       fileContent: String, fileName: String):
  (Set[(String, String)], List[Set[ExternalType]]) = {
    val parser = new MethodVisitor()
    parser.parse(fileContent, fileName)
    val imports: Set[(String, String)] = getImports(parser, excludePackages)
    val importsSet = imports.map(tuple2ToImportString)
    import com.kodebeagle.parser.MethodVisitorHelper._
    val tokensMap: List[Map[String, List[ExternalLine]]] = getTokenMap(parser, importsSet)
    (imports, createMethodIndexEntries(parser, tokensMap))
  }

  def getImports(parser: MethodVisitor, excludePackages: Set[String]): Set[(String, String)] = {
    import scala.collection.JavaConversions._
    val iterOfPackageAndImport = parser.getImportDeclMap.toIterator
      .map(x => (x._2.stripSuffix(s".${x._1}"), x._1))
    handleInternalImport(iterOfPackageAndImport, excludePackages)
  }

  protected def handleInternalImport(iterOfPackageAndImport: Iterator[(String, String)],
                                     packages: Set[String]): Set[(String, String)] = {
    iterOfPackageAndImport.filterNot { case (left, right) => packages.contains(left) }.toSet
  }

  def generateTypeReferences(files: Map[String, String], excludePackages: List[String],
                             repo: Option[Repository]): Set[TypeReference] = {
    var indexEntries = immutable.HashSet[ExternalTypeReference]()
    val r = repo.getOrElse(Repository.invalid)
    for (file <- files) {
      val (fileName, fileContent) = file
      val fullGithubURL = fileNameToURL(r, fileName)
      try {
        val (imports, methodTokens) =
          extractTokensASTParser(excludePackages.toSet, fileContent, fileName)
        val score =
          if (isTestFile(imports)) r.stargazersCount / penalizeTestFiles else r.stargazersCount
        for (methodToken <- methodTokens) {
          indexEntries =
            indexEntries + ExternalTypeReference(r.id, fullGithubURL, methodToken,
              score, r.tag)
        }
      } catch {
        case e: Throwable => log.error(s"Failed for $fullGithubURL", e);
      }
    }
    indexEntries.filter(_.types.nonEmpty).toSet
  }
}

