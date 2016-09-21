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

case class Line(line: Int, startCol: Int, endCol: Int)

case class ContextProperty(name: String) extends Property

case class PayloadProperty(name: String, lines: Set[Line]) extends Property

case class ContextType(name: String, props: Set[ContextProperty]) extends Type {
  type T = ContextProperty
}

case class PayloadType(name: String, props: Set[PayloadProperty]) extends Type {
  type T = PayloadProperty
}

case class Payload(types: Set[PayloadType], score: Long, file: String)

case class Context(text: String, types: Set[ContextType])

case class TypeReference(contexts: Set[Context], payload: Payload, score: Long, file: String)

case class SourceFile(repoId: Long, fileName: String, fileContent: String)

case class RepoFileNameInfo(login: String, id: Int, name: String, fork: Boolean, language: String,
                            defaultBranch: String, stargazersCount: Int)

case class Repository(login: String, id: Int, name: String, fork: Boolean, language: String,
                      defaultBranch: String, stargazersCount: Int, sloc: Int, fileCount: Int,
                      size: Long)

case class Statistics(sloc: Int, fileCount: Int, size: Long)

/** For testing */
object Repository {
  def invalid: Repository =
    Repository("n-a", -1, "n-a", fork = false, "Java", "n-a", 0, -1, -1, -1)
}

/* File Metadata related entities */
case class TypeDeclaration(fileType: String, loc: Line)

// for each import -- its fqt, its var locations, its methods and their locations
case class ExternalRef(fqt: String, vars: Set[Line], methods: Set[MethodTypeLocation])

case class MethodTypeLocation(loc: Set[Line], method: String, argTypes: List[String])

// method definitions in this file
case class MethodDefinition(loc: Line, method: String, argTypes: List[String])

// p = ParentLine, c = all internal child refs of that parent
case class InternalRef(p: Line, c: Set[Line])

case class SuperTypes(superClass: Map[String, String], interfaces: Map[String, List[String]])

case class FileMetaData(repoId: Long, fileName: String,
                        superTypes: SuperTypes,
                        fileTypes: List[TypeDeclaration],
                        externalRefList: List[ExternalRef],
                        methodDefinitionList: List[MethodDefinition],
                        internalRefList: List[InternalRef])

case class JavaFileIndices(searchableRefs: Set[TypeReference],
                           fileMetaData: FileMetaData, sourceFile: SourceFile, repo: String)

case class MethodType(returnType: String, methodName: String, argTypes: List[String],
                      isDeclared: Boolean, isConstructor: Boolean)

case class TypesInFile(repoName: String, fileName: String,
                       // imported types -> (varnames, methods)
                       usedTypes: Map[String, (Set[String], Set[MethodType])],
                       // declared types -> methods
                       declaredTypes: Map[String, Set[MethodType]])

case class PropertyDocs(propertyName: String, argTypes: List[String],
                        propertyDoc: String) extends Property

case class TypeDocsIndices(typeName: String, typeDoc: String,
                           propertyDocs: Set[PropertyDocs]) extends Property

case class Docs(fileName: String, typeDocs: Set[TypeDocsIndices], score: Long)
