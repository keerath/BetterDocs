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

import java.util.regex.Pattern

import scala.collection.{Iterable, mutable}

// for aggregation tasks on basis of types
class TypeAggregator() extends Serializable {

  import TypeAggregator._

  // # used in other types (method usage will be higher)
  var score = 0

  var declaredInFile: Option[String] = None
  var declaredInRepo: Option[String] = None
  var usedInReposCount: mutable.Map[String, Int] = mutable.Map.empty[String, Int]

  var varNames: mutable.Map[String, Int] = mutable.Map.empty[String, Int]
  val methods: mutable.Set[MethodType] = mutable.Set.empty[MethodType]
  val methodCount: mutable.Map[(String, Int), Int] = mutable.Map.empty[(String, Int), Int]

  def merge(other: TypeAggregator): TypeAggregator = {
    score += other.score
    other.declaredInFile match {
      case Some(d) => declaredInFile = other.declaredInFile
      case None =>
    }
    other.declaredInRepo match {
      case Some(d) => declaredInRepo = other.declaredInRepo
      case None =>
    }
    mergeMethods(other.methods)
    other.methodCount.foreach(m => methodCount.update(m._1, methodCount.getOrElse(m._1, 0) + m._2))
    other.varNames.foreach(e => varNames.update(e._1, varNames.getOrElse(e._1, 0) + e._2))
    // A bit of a hack to manage the count map size on the combiner
    // If after combining other agg, the map size goes over 20, then trim it to remove mins.
    varNames = trimMap(varNames, 50)

    other.usedInReposCount.foreach(e =>
      usedInReposCount.update(e._1, usedInReposCount.getOrElse(e._1, 0) + e._2))
    // A bit of a hack to manage the count map size on the combiner
    // If after combining other agg, the map size goes over 20, then trim it to remove mins.
    usedInReposCount = trimMap(usedInReposCount, 50)
    this
  }

  def merge(vars: Set[String], ms: Set[MethodType], repo: String, file: String): TypeAggregator = {
    score += 1
    // removing the constructor invocations
    mergeMethods(ms.filter(e => !e.isConstructor))
    vars.foreach(e => varNames.update(e, varNames.getOrElse(e, 0) + 1))
    usedInReposCount.update(repo, usedInReposCount.getOrElse(repo, 0) + 1)
    ms.foreach(m => {
      // Only if this method is not a decl, it counts towards usage.
      m.isDeclared match {
        case false => methodCount.update((m.methodName, m.argTypes.size),
          methodCount.getOrElse((m.methodName, m.argTypes.size), 0) + 1)
        case true => {
          // this is redundant for doing no each invocation inside ms but not much harm
          declaredInFile = Option(file)
          declaredInRepo = Option(repo)
        }
      }
    })

    this
  }

  /**
    * Method Aggregation strategy:
    * 1. If method type is declared, check if a method already exists with same name
    * and number of arguments. If yes, remove the earlier method if it is not a
    * declared method type. Finally add the new method type.
    * 2. If method type is used, check if the method with name and type numbers
    * already exists, if yes check if we have more specific types of args (Object-> Specific),
    * if yes, then replace the older one with new method else skip.
    */
  // scalastyle:off
  private def mergeMethods(otherMethods: Iterable[MethodType]): Unit = {
    otherMethods.foreach(om => om.isDeclared match {
      // 1.
      case true => {
        // Find an already encountered method with particular name, and args size
        // that was not marked as a declared method
        val existingMethod = methods.find(m => m.methodName.equals(om.methodName) &&
          m.argTypes.size == om.argTypes.size && !m.isDeclared)
        existingMethod match {
          case Some(m) => {
            methods remove m
            methods add om
          }
          case None => methods add om
        }
      }
      // 2.
      case false => {
        val existingMethod = methods.find(m => m.methodName.equals(om.methodName) &&
          m.argTypes.size == om.argTypes.size)
        existingMethod match {
          case Some(m) => if (!m.isDeclared) {
            // handle the case where existing infered method is present
            // and we have to decide how to reconcile it with the current method
            // In case the existing method is declared, we do nothing (no else block).
            if (isBetterMethodDefinition(m, om)) {
              methods remove m
              methods add om
            }
          }
          // There's no matching method encountered yet, add it
          case None => methods add om
        }
      }
    })
  }

  // scalastyle:on

  // Figure out if second method type argument is a better version of the first one?
  private def isBetterMethodDefinition(m: MethodType, om: MethodType): Boolean = {
    var score = 0
    m.argTypes.zip(om.argTypes).foreach(pair => {
      if (pair._1.equalsIgnoreCase(OBJ_TYPE) &&
        !pair._2.equalsIgnoreCase(OBJ_TYPE)) {
        // second method is better for this arg type
        score += 1
      } else if (pair._2.equalsIgnoreCase(OBJ_TYPE) &&
        !pair._1.equalsIgnoreCase(OBJ_TYPE)) {
        // first method is better for this arg type
        score -= 1
      }
    })
    score > 0
  }

  def randomString: String = {
    val r = scala.util.Random
    r.nextInt(10000).toString
  }

  def result(typeName: String, propDocs: Set[PropertyDocs]): TypeAggregation = {
    val tokens = typeName.split("\\.")

    val srchText = methods.map(m => {
      val typePrefix = s"${camelCasePattern.split(typeName).mkString(" ")}"
      val methodPrefix = s"${camelCasePattern.split(m.methodName).mkString(" ")}"

      val methodText = m.argTypes
        .map(e => camelCasePattern.split(e.split("\\.").last).mkString(" ")).mkString(" ")
      // considering overloaded methods check name and arg types
      val methodDoc = propDocs.find(propDoc => propDoc.propertyName == m.methodName &&
        propDoc.argTypes == m.argTypes).getOrElse("")

      s"$methodDoc $typePrefix $methodPrefix $methodText"
    }).toSet

    TypeAggregation(typeName, score,
      context = tokens.slice(0, tokens.length - 1).toSet,
      typeSuggest = CompletionSuggest(
        camelCasePattern.split(typeName).toSet[String].map(e => s"$e$randomString") + typeName,
        typeName, score),
      methodSuggest = PayloadCompletionSuggest(
        methods.map(_.methodName).toSet, score,
        // Coz: https://issues.scala-lang.org/browse/SI-6476
        """{"type": """" + typeName + """"}"""),
      searchText = srchText,
      vars = varNames.filter(!_._1.isEmpty).map(e => VarCount(e._1, e._2)).toSet,
      methods = methodCount.map(e => MethodCount(e._1._1, e._1._2, e._2)).toSet,
      declaredInFile.getOrElse(""),
      declaredInRepo.getOrElse(""),
      usedInReposCount.map(e => RepoCount(e._1, e._2)).toSet)
  }
}

object TypeAggregator {
  val OBJ_TYPE = "java.lang.Object"
  val camelCasePattern = Pattern.compile(
    """([^\p{L}\d]+)|(?<=\D)(?=\d)|(?<=\d)(?=\D)|(?<=[\p{L}&&[^\p{Lu}]])"""
      +
      """(?=\p{Lu})|(?<=\p{Lu})(?=\p{Lu}[\p{L}&&[^\p{Lu}]])""")

  def trimMap[K](map: mutable.Map[K, Int], size: Int): mutable.Map[K, Int] = {
    if (map.size > size) {
      val threshold = map.values.toList.sortWith(_ > _).take(size).last
      val min = map.values.min
      map.retain((k, v) => v > threshold)
    }
    map
  }
}

// For Type aggregations
case class TypeAggregation(name: String, score: Int, context: Set[String],
                           typeSuggest: CompletionSuggest,
                           methodSuggest: PayloadCompletionSuggest,
                           searchText: Set[String], vars: Set[VarCount],
                           methods: Set[MethodCount],
                           declInFile: String,
                           declInRepo: String,
                           repoCounts: Set[RepoCount])

case class MethodCount(name: String, params: Int, count: Int)

case class RepoCount(name: String, count: Int)

case class CompletionSuggest(input: Set[String], output: String, weight: Int);

case class PayloadCompletionSuggest(input: Set[String], weight: Int, payload: String)

case class VarCount(name: String, count: Int)

