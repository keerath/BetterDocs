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
import java.util.Map.Entry

import com.kodebeagle.javaparser.MethodInvocationResolver.{MethodJavadoc, TypeJavadoc}
import com.kodebeagle.javaparser.SingleClassBindingResolver
import com.kodebeagle.logging.Logger
import com.kodebeagle.util.Utils
import org.eclipse.jdt.core.dom.{ASTNode, CompilationUnit}

import scala.collection.JavaConversions._
import scala.collection.mutable

object FileMetaDataIndexHelper extends Logger {

  def generateMetaData(resolver: SingleClassBindingResolver,
                       unit: CompilationUnit, repoId: Long, fileName: String): FileMetaData = {
    val typesAtPos = resolver.getTypesAtPosition

    val allImports: Set[String] = getExtImports(resolver, typesAtPos)

    // typeLocationList for variables
    val extRefList = getExtRefList(unit, resolver, allImports)

    // method definition in that class
    val methodDefinitionList = getMethodDefinitionList(unit, resolver)

    // internal references
    val internalRefsList = getInternalRefs(unit, resolver)
    val fileTypes = getFileTypes(unit, resolver)
    val superTypes = SuperTypes(resolver.getSuperType.toMap,
      resolver.getInterfaces.toMap.mapValues(_.toList))
    FileMetaData(repoId, fileName, superTypes, fileTypes.toList,
      extRefList, methodDefinitionList.toList, internalRefsList.toList)
  }

  private def getMethodDefinitionList(unit: CompilationUnit,
                                      resolver: SingleClassBindingResolver):
  mutable.Buffer[MethodDefinition] = {
    for (m <- resolver.getDeclaredMethods) yield {
      val line: Integer = unit.getLineNumber(m.getLocation)
      val col: Integer = unit.getColumnNumber(m.getLocation)
      MethodDefinition(Line(line, col, 0), m.getMethodName, m.getArgs.values().toList)
    }
  }

  private def getExtImports(resolver: SingleClassBindingResolver,
                            typesAtPos: util.Map[ASTNode, String]): Set[String] = {
    val externalRefs = scala.collection.mutable.Set[String]()
    val imports = resolver.getImportsDeclarationNode.values()
    typesAtPos.entrySet.foreach(e => externalRefs.add(e.getValue.toString))
    imports.foreach(e => externalRefs.add(e))
    externalRefs.toSet
  }

  private def getFileTypes(unit: CompilationUnit,
                           r: SingleClassBindingResolver): mutable.Buffer[TypeDeclaration] = {
    val types: util.Map[String, String] = r.getClassesInFile
    for (typeDeclaration <- r.getTypeDeclarations) yield {
      TypeDeclaration(types.get(typeDeclaration.getClassName),
        Line(unit.getLineNumber(typeDeclaration.getLoc),
          unit.getColumnNumber(typeDeclaration.getLoc), 0))
    }
  }

  private def getInternalRefs(unit: CompilationUnit,
                              resolver: SingleClassBindingResolver): List[InternalRef] = {
    resolver.getVariableDependencies.entrySet
      .foldLeft(mutable.Map.empty[Line, mutable.Set[Line]])((agg, value) => {
        val (child, parent) = (value.getKey, value.getValue)
        val chlineNo: Integer = unit.getLineNumber(child.getStartPosition)
        val chcol: Integer = unit.getColumnNumber(child.getStartPosition)
        val chlength: Integer = child.getLength
        val plineNo: Integer = unit.getLineNumber(parent.getStartPosition)
        val pcol: Integer = unit.getColumnNumber(parent.getStartPosition)

        val parentLine = Line(plineNo, pcol, 0)
        val childLine = Line(chlineNo, chcol, chlength)
        val children = agg.getOrElse(parentLine, mutable.Set.empty)

        if (!(chlineNo == plineNo) || !(chcol == pcol)) {
          // only update if child is diff from parent
          children.add(childLine)
          agg.update(parentLine, children)
        }

        agg
      }).map(e => InternalRef(e._1, e._2.toSet)).toList

  }

  // scalastyle:off
  private def getExtRefList(unit: CompilationUnit, resolver: SingleClassBindingResolver,
                            imports: Set[String]): List[ExternalRef] = {
    val impVsVarLocMap: mutable.Map[String, mutable.Set[Line]] = mutable.Map.empty

    def updateMap(e: Entry[ASTNode, String]): Unit = {
      val line: Integer = unit.getLineNumber(e.getKey.getStartPosition)
      val col: Integer = unit.getColumnNumber(e.getKey.getStartPosition)

      val set = impVsVarLocMap.getOrElse(e.getValue, mutable.Set.empty[Line])
      set.add(Line(line, col, e.getKey.getLength))
      impVsVarLocMap.update(e.getValue, set)
    }
    for (e <- resolver.getImportsDeclarationNode.entrySet) yield {
      updateMap(e)
    }

    for (e <- resolver.getTypesAtPosition.entrySet) yield {
      updateMap(e)
    }

    val methodInvokMap = resolver.getMethodInvoks
    val typeVsMethods = methodInvokMap.entrySet.flatMap(_.getValue).map(m => {
      val loc: Integer = m.getLocation
      val line: Integer = unit.getLineNumber(loc)
      val col: Integer = unit.getColumnNumber(loc)
      (m.getTargetType, MethodTypeLocation(Set(Line(line, col, m.getLength)),
        m.getMethodName, m.getArgTypes.toList))
    }).foldLeft(mutable.Map.empty[String, mutable.Set[MethodTypeLocation]])((agg, value) => {
      val (typ, mth) = value
      val methLocSet = agg.getOrElse(typ, mutable.Set.empty[MethodTypeLocation])
      val mtch = methLocSet.find(e =>
        (e.method.equals(mth.method) && e.argTypes.size.equals(mth.argTypes.size)))

      def betterType(t1: List[String], t2: List[String]) = {
        t1.zip(t2).map(e => {
          if (e._1.equalsIgnoreCase("java.lang.Object")) e._2
          else e._1
        })
      }

      mtch match {
        case Some(m) => {
          methLocSet.remove(m)
          methLocSet.add(MethodTypeLocation(m.loc ++ mth.loc, m.method,
            betterType(m.argTypes, mth.argTypes)))
        }
        case None => {
          methLocSet.add(mth)
        }
      }
      agg.update(typ, methLocSet)
      agg
    })

    imports.map(e => ExternalRef(e, impVsVarLocMap.getOrElse(e, Set.empty).toSet,
      typeVsMethods.getOrElse(e, Set.empty).toSet)).toList
  }

  // scalastyle:on
}

object TypesInFileIndexHelper extends Logger {

  // For intermediate indices
  def usedTypesInFile(scbr: SingleClassBindingResolver):
  Map[String, (Set[String], Set[MethodType])] = {
    // for each method invok in file, aggregate it by folding left
    scbr.getMethodInvoks.toMap.flatMap(_._2).foldLeft(
      new mutable.HashMap[String, (Set[String], Set[MethodType])]())(
      // sequence operation
      (agg, mthd) => {
        val valOpt = agg.get(mthd.getTargetType)
        valOpt match {
          // If value for a type already exists in the map, then get the existing value,
          // modify it and update the same map.
          case Some(v) => {
            val updatedVal = (v._1 + mthd.getTarget,
              v._2 + MethodType(mthd.getReturnType, mthd.getMethodName,
                mthd.getArgTypes.toList, false, mthd.getConstructor))
            agg.update(mthd.getTargetType, updatedVal)
          }
          // If value doesn't already exist, create and add to the map.
          case None => {
            agg.put(mthd.getTargetType, (Set(mthd.getTarget),
              Set(MethodType(mthd.getReturnType, mthd.getMethodName,
                mthd.getArgTypes.toList, false, mthd.getConstructor))))
          }
        }
        agg
      }
    ).toMap
  }

  def declaredTypesInFile(scbr: SingleClassBindingResolver): Map[String, Set[MethodType]] = {
    scbr.getDeclaredMethods.foldLeft(new mutable.HashMap[String, Set[MethodType]]())(
      (agg, mthd) => {
        val valOpt = agg.get(mthd.getEnclosingType)
        valOpt match {
          case Some(v) => {
            agg.update(mthd.getEnclosingType,
              v + MethodType(mthd.getReturnType, mthd.getMethodName,
                mthd.getArgs.values().toList, true, Option(mthd.getReturnType).isDefined))
          }
          case None => {
            agg.put(mthd.getEnclosingType,
              Set(MethodType(mthd.getReturnType, mthd.getMethodName,
                // we will loose the param order here.
                mthd.getArgs.values().toList, true, Option(mthd.getReturnType).isDefined)))
          }
        }
        agg
      }
    ).toMap
  }
}

object ExternalRefsIndexHelper extends Logger {

  def extractTypeReference(scbr: SingleClassBindingResolver, cu: CompilationUnit,
                           score: Long, repoFileLocation: String): TypeReference = {
    val contexts = generateContexts(scbr)
    val payload = generatePayload(scbr, cu, score, repoFileLocation)
    TypeReference(contexts, payload, score, repoFileLocation)
  }


  private def generateContexts(scbr: SingleClassBindingResolver): Set[Context] = {
    import scala.collection.JavaConversions._
    scbr.getMethodInvoks.map { case (methodDecl, methodCalls) =>
      // for every method in the class
      val contextTypes = methodCalls.groupBy(_.getTargetType)
        // then map the grp to:
        .map { case (typ, grp) =>
        val props = grp.groupBy(_.getMethodName).keys.map(ContextProperty).toSet
        ContextType(typ, props)
      }.toSet
      Context(Utils.generateMethodSignature(methodDecl), contextTypes)
    }.toSet
  }


  private def generatePayload(scbr: SingleClassBindingResolver, cu: CompilationUnit,
                              score: Long, repoFileLocation: String): Payload = {
    import scala.collection.JavaConversions._
    val payloadTypes = scbr.getMethodInvoks.values().flatten
      .groupBy(_.getTargetType).map { case (typ, grp) =>
      val propSet = grp.groupBy(_.getMethodName).map { case (prop, pgrp) =>
        PayloadProperty(prop, pgrp.map(t => {
          val line = cu.getLineNumber(t.getLocation)
          val col = cu.getColumnNumber(t.getLocation)
          Line(line, col, col + t.getLength)
        }).toSet)
      }.toSet
      PayloadType(typ, propSet)
    }.toSet
    Payload(payloadTypes, score, repoFileLocation)
  }
}

object JavaDocIndexHelper {

  def generateJavaDocs(repoId: Long, repoFileLocation: String,
                       resolver: SingleClassBindingResolver): Set[TypeDocsIndices] = {

    val commentIndices = mutable.Set.empty[TypeDocsIndices]

    for (javadoc: TypeJavadoc <- resolver.getTypeJavadocs) {

      val methodJavaDocs = mutable.Set.empty[PropertyDocs]

      for (methodJavadoc: MethodJavadoc <- javadoc.getMethodJavadocs) {
        methodJavaDocs.add(PropertyDocs(methodJavadoc.getName,
          methodJavadoc.getArgTypes.toList, methodJavadoc.getComment))
      }

      commentIndices.add(TypeDocsIndices(javadoc.getName,
        javadoc.getComment, methodJavaDocs.toSet))

    }

    commentIndices.toSet
  }
}

