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
import com.kodebeagle.parser.MethodVisitor

import scala.collection.immutable
import scala.util.Try

class JavaInternalTypeRefIndexer extends JavaExternalTypeRefIndexer {

  override protected def handleInternalImport(iterOfPackageAndImport: Iterator[(String, String)],
                                              packages: Set[String]): Set[(String, String)] = {
    iterOfPackageAndImport.filter { case (left, right) => packages.contains(left) }.toSet
  }


  override def generateTypeReferences(files: Map[String, String],
                                      internalPackages: List[String],
                                      repo: Option[Repository]): Set[TypeReference] = {
    var indexEntries = immutable.HashSet[InternalTypeReference]()
    val r = repo.getOrElse(Repository.invalid)
    for (file <- files) {
      val (fileName, fileContent) = file
      val lines = fileContent.split("\\r?\\n")
      val fullGithubURL = fileNameToURL(r, fileName)
      try {
        val (imports, methodTokens) =
          extractTokensASTParser(internalPackages.toSet, fileContent, fileName)
        val score =
          if (isTestFile(imports)) r.stargazersCount / penalizeTestFiles else r.stargazersCount
        for (methodToken <- methodTokens) {
          val internalMethodToken =
            methodToken.map(x => InternalType(x.typeName,
              toInternalHighlighters(lines, x.lines), x.properties))
          indexEntries = indexEntries +
            InternalTypeReference(r.id, fullGithubURL, internalMethodToken, score, r.tag)
        }
      } catch {
        case e: Throwable => log.error(s"Failed for $fullGithubURL", e);
      }
    }
    indexEntries.filter(_.types.nonEmpty).toSet
  }

  private def toInternalHighlighters(lines: Array[String], highlighters: List[ExternalLine]) = {
    highlighters.map(highlighter => Try(InternalLine(lines(highlighter.lineNumber - 1),
      highlighter.lineNumber, highlighter.startColumn,
      highlighter.endColumn))).filter(_.isSuccess).map(_.get)
  }
}
