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

import java.util
import java.util.{List => JList}

import com.kodebeagle.javaparser.JavaASTParser.ParseType
import com.kodebeagle.javaparser.{JavaASTParser, MethodInvocationResolver, SingleClassBindingResolver}
import com.kodebeagle.logging.Logger
import org.eclipse.jdt.core.dom.{ASTNode, CompilationUnit}

import scala.collection.mutable

case class RepoSource(repoId: Long, fileName: String, fileContent: String)

case class TypeDeclaration(fileType: String, loc: String, superTypes: SuperTypes)

case class ExternalRef(id: Int, fqt: String)

case class VarTypeLocation(loc: String, id: Int)

case class MethodTypeLocation(loc: String, id: Int, method: String, argTypes: List[String])

case class MethodDefinition(loc: String, method: String, argTypes: List[String])

case class InternalRef(childLine: String, parentLine: String)

case class SuperTypes(superClass: String, interfaces: JList[Object])

case class FileMetaData(repoId: Long, fileName: String,
                        fileTypes: util.List[TypeDeclaration],
                        externalRefList: List[ExternalRef],
                        typeLocationList: List[VarTypeLocation],
                        methodTypeLocation: List[MethodTypeLocation],
                        methodDefinitionList: List[MethodDefinition],
                        internalRefList: List[InternalRef])


object FileMetaDataIndexer extends Logger {

  val parser = new JavaASTParser(true)
  import scala.collection.JavaConversions._

  def generateMetaData(source: SourceFile): Set[FileMetaData] = {
    val indexEntry = mutable.Set[FileMetaData]()
    val cu: ASTNode = parser.getAST(source.fileContent, ParseType.COMPILATION_UNIT)
    if (Option(cu).isDefined) {
      val unit: CompilationUnit = cu.asInstanceOf[CompilationUnit]
      val resolver: SingleClassBindingResolver = new SingleClassBindingResolver(unit)
      resolver.resolve
      val typesAtPos = resolver.getTypesAtPosition
      // External reference
      val idVsExternalRefs: Map[String, Int] = getExternalRefs(resolver, typesAtPos)
      val externalRefsList = idVsExternalRefs.map(x => ExternalRef(x._2, x._1))
      // typeLocationList for variables
      val typeLocationVarList = getTypeLocationVarList(unit, typesAtPos, idVsExternalRefs)
      // importLocationList for imports
      val importLocationList = getImportLocationList(unit, resolver, idVsExternalRefs)
      // typelocation for method call expression
      val typeLocationMethodList =
        getTypeLocationMethodList(unit, resolver.getMethodInvoks, idVsExternalRefs)
      // method definition in that class
      val methodDefinitionList = getMethodDefinitionList(unit, resolver)
      // internal references
      val internalRefsList = getInternalRefs(unit, resolver)
      val fileTypes = getFileTypes(unit, resolver)
      indexEntry += FileMetaData(source.repoId, source.fileName, fileTypes.toList,
        externalRefsList.toList, typeLocationVarList.toList ++ importLocationList.toList,
        typeLocationMethodList.toList, methodDefinitionList.toList, internalRefsList.toList)
    } else {
      log.info("Unable to create AST for file " + source.fileName +
        "and file contents are \n" + source.fileContent)
    }
    indexEntry.toSet
  }

  def getMethodDefinitionList(unit: CompilationUnit, resolver: SingleClassBindingResolver):
  mutable.Buffer[MethodDefinition] = {
    for (m <- resolver.getDeclaredMethods) yield {
      val line: Integer = unit.getLineNumber(m.getLocation)
      val col: Integer = unit.getColumnNumber(m.getLocation)
      MethodDefinition(line + "#" + col, m.getMethodName, m.getArgTypes.toList)
    }
  }

  def getExternalRefs(resolver: SingleClassBindingResolver,
                      typesAtPos: util.Map[ASTNode, String]): Map[String, Int] = {
    val externalRefs = scala.collection.mutable.Set[String]()
    for (e <- typesAtPos.entrySet) {
      externalRefs.add(e.getValue.toString)
    }

    val imports = resolver.getImportsDeclarationNode.values()
    externalRefs ++ imports
    externalRefs.map(resolver.rmSpecialSymbols).zipWithIndex.toMap
  }

  def getFileTypes(unit: CompilationUnit, resolver: SingleClassBindingResolver):
  mutable.Buffer[TypeDeclaration] = {
    val types: util.Map[String, String] = resolver.getClassesInFile
    val superTypes = resolver.getClassNameVsSuperTypes
    for (typeDeclaration <- resolver.getTypeDeclarations) yield {
      TypeDeclaration(types.get(typeDeclaration.getClassName),
        unit.getLineNumber(typeDeclaration.getLoc) + "#"
          + unit.getColumnNumber(typeDeclaration.getLoc),
        superTypes.get(typeDeclaration.getClassName))
    }
  }

  def getTypeLocationVarList(unit: CompilationUnit, typesAtPos: util.Map[ASTNode, String],
                             idVsExternalRefs: Map[String, Int]):
  scala.collection.mutable.Set[VarTypeLocation] = {
    for (e <- typesAtPos.entrySet) yield {
      val line: Integer = unit.getLineNumber(e.getKey.getStartPosition)
      val col: Integer = unit.getColumnNumber(e.getKey.getStartPosition)
      VarTypeLocation(line + "#" + col + "#" + e.getKey.getLength,
        idVsExternalRefs.getOrElse(e.getValue, -1))
    }
  }

  def getTypeLocationMethodList(unit: CompilationUnit,
                                methodInvokMap: util.Map[String,
                                  util.List[MethodInvocationResolver.MethodInvokRef]],
                                idVsExternalRefs: Map[String, Int]):
  scala.collection.mutable.Set[MethodTypeLocation] = {
    for {entry <- methodInvokMap.entrySet
         m <- entry.getValue
    } yield {
      val loc: Integer = m.getLocation
      val line: Integer = unit.getLineNumber(loc)
      val col: Integer = unit.getColumnNumber(loc)
      MethodTypeLocation(line + "#" + col + "#" + m.getLength,
        idVsExternalRefs.getOrElse(m.getTargetType, -1),
        m.getMethodName, m.getArgTypes.toList)
    }
  }

  def getInternalRefs(unit: CompilationUnit, resolver: SingleClassBindingResolver):
  scala.collection.mutable.Set[InternalRef] = {
    for (e <- resolver.getVariableDependencies.entrySet) yield {
      val child: ASTNode = e.getKey
      val chline: Integer = unit.getLineNumber(child.getStartPosition)
      val chcol: Integer = unit.getColumnNumber(child.getStartPosition)
      val chlength: Integer = child.getLength
      val parent: ASTNode = e.getValue
      val pline: Integer = unit.getLineNumber(parent.getStartPosition)
      val pcol: Integer = unit.getColumnNumber(parent.getStartPosition)
      InternalRef(chline + "#" + chcol + "#" + chlength, pline + "#" + pcol)
    }
  }

  def getImportLocationList(unit: CompilationUnit, resolver: SingleClassBindingResolver,
                            idVsExternalRefs: Map[String, Int]): mutable.Set[VarTypeLocation] = {
    for (e <- resolver.getImportsDeclarationNode.entrySet) yield {
      val line: Integer = unit.getLineNumber(e.getKey.getStartPosition)
      val col: Integer = unit.getColumnNumber(e.getKey.getStartPosition)
      VarTypeLocation(line + "#" + col + "#" + e.getKey.getLength,
        idVsExternalRefs.getOrElse(e.getValue, -1))
    }
  }
}
