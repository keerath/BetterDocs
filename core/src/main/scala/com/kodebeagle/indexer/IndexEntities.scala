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

import scala.collection.immutable

trait Line {
  def lineNumber: Int
  def startColumn: Int
  def endColumn: Int
}

trait Type {
  type T <: Line
  def typeName: String
  def lines: List[T]
  def properties: Set[Property]
}

abstract class TypeReference {
  type T <: Type
  def repoId: Int
  def file: String
  def types: Set[T]
  def score: Int
  def tag: String
}


case class InternalTypeReference(repoId: Int, file: String,
                                 types: Set[InternalType],
                                 score: Int, tag: String) extends TypeReference {
  type T = InternalType
}

case class ExternalTypeReference(repoId: Int, file: String,
                                 types: Set[ExternalType],
                                 score: Int, tag: String) extends TypeReference {
  type T = ExternalType
}

case class InternalType(typeName: String, lines: List[InternalLine],
                        properties: Set[Property]) extends Type {
  type T = InternalLine
}

case class ExternalType(typeName: String, lines: List[ExternalLine],
                        properties: Set[Property]) extends Type {
  type T = ExternalLine
}

case class InternalLine(line: String, lineNumber: Int,
                        startColumn: Int, endColumn: Int) extends Line

case class ExternalLine(lineNumber: Int, startColumn: Int, endColumn: Int) extends Line

case class Property(propertyName: String, lines: List[Line])

case class Token(importName: String, importExactName: String,
                 lineNumbers: immutable.Set[ExternalLine])


case class SourceFile(repoId: Int, fileName: String, fileContent: String, tag: String)

case class RepoFileNameInfo(login: String, id: Int, name: String, fork: Boolean, language: String,
                            defaultBranch: String, stargazersCount: Int, tag: String)

case class Repository(login: String, id: Int, name: String, fork: Boolean, language: String,
                      defaultBranch: String, stargazersCount: Int, sloc: Int, fileCount: Int,
                      size: Long, tag: String)

case class Statistics(sloc: Int, fileCount: Int, size: Long)

/** For testing */
object Repository {
  def invalid: Repository =
    Repository("n-a", -1, "n-a", fork = false, "Java", "n-a", 0, -1, -1, -1, "")
}

