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

package com.kodebeagle.parser

import com.kodebeagle.indexer.{ExternalLine, ExternalType, ExternalTypeReference, Property, Repository}
import com.kodebeagle.logging.Logger
import com.kodebeagle.util.SparkIndexJobHelper
import org.mozilla.javascript.ast.{AstNode, AstRoot, ErrorCollector, FunctionCall, NodeVisitor, PropertyGet, VariableInitializer}
import org.mozilla.javascript.{CompilerEnvirons, IRFactory}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.Try

case class RefVariable(refVariableName: String, lineNumber: Int)

// Assuming the commonjs module format for any given npm package
object JsParser extends Logger {
  val env = new CompilerEnvirons()
  env.setRecoverFromErrors(true)
  env.setIdeMode(true)
  env.setErrorReporter(new ErrorCollector)
  env.setStrictMode(false)

  private def isRequireCall(astNode: AstNode) = {
    astNode match {
      case call: FunctionCall => if (call.getTarget.toSource() == "require") {
        call.getArguments.headOption match {
          case Some(x) => !x.toSource().contains("/")
          case None => false
        }
      }
      else {
        false
      }
      case _ => false
    }
  }

  private def isRequireString(arg: String) = {
    val isQuoted = arg.startsWith("'") && arg.endsWith("'")
    val unquotedString = arg.replaceAll("'", "")
    unquotedString.forall(_.isLetter) && isQuoted
  }

  // scalastyle:off  cyclomatic.complexity
  private def getMapOfRefVarAndType(rootNode: AstRoot) = {
    val mapOfRefVarAndType = new mutable.HashMap[RefVariable, String]()
    rootNode.visit(new NodeVisitor {
      override def visit(astNode: AstNode): Boolean = {
        astNode match {
          case init: VariableInitializer => val variable = init.getTarget
            init.getInitializer match {
              case call: FunctionCall => if (isRequireCall(call)) {
                call.getArguments.headOption match {
                  case Some(x) => val argument = x.toSource()
                    if (isRequireString(argument)) {
                      mapOfRefVarAndType += ((RefVariable(variable.toSource(),
                        variable.getLineno), argument.replaceAll("'", "")))
                    }
                  case None =>
                }
              }
              case propGet: PropertyGet => val target: AstNode = propGet.getTarget
                if (isRequireCall(target)) {
                  target.asInstanceOf[FunctionCall].getArguments.headOption match {
                    case Some(x) => val argument = x.toSource()
                      if (isRequireString(argument)) {
                        mapOfRefVarAndType += ((RefVariable(variable.toSource(),
                          variable.getLineno), argument.replaceAll("'", "")
                          ++ "." ++ propGet.getProperty.toSource()))
                      }
                    case None =>
                  }
                }
                else {
                  mapOfRefVarAndType.keySet.find(_.refVariableName == target.toSource()) match {
                    case Some(x) => mapOfRefVarAndType += ((RefVariable(variable.toSource(),
                      variable.getLineno), mapOfRefVarAndType.get(x).get
                      ++ "." ++ propGet.getProperty.toSource()))
                    case None =>
                  }
                }
              case _ =>
            }
          case _ =>
        }
        true
      }
    })
    mapOfRefVarAndType.toMap
  }

  def getProperties(rootNode: AstRoot, variable: String): List[AstNode] = {
    val set = new scala.collection.mutable.HashSet[AstNode]()
    rootNode.visit(new NodeVisitor {
      override def visit(astNode: AstNode): Boolean = {
        astNode match {
          case propGet: PropertyGet => if (propGet.getTarget.toSource() == variable) {
            set += propGet.getProperty
          }
          case _ =>
        }
        true
      }
    })
    set.toList
  }

  private def toHighLighter(lineNumbers: List[Int]): List[ExternalLine] = {
    lineNumbers.map(ExternalLine(_, -1, -1))
  }

  def getTypes(rootNode: AstRoot): Iterable[ExternalType] = {
    val mapOfRefVarAndType = getMapOfRefVarAndType(rootNode)
    mapOfRefVarAndType.map { tupleOfVarLineType =>
      val props = getProperties(rootNode, tupleOfVarLineType._1.refVariableName)
      val propLines = props.map(prop => (prop.toSource(), prop.getLineno)).groupBy(_._1)
        .mapValues(_.map(_._2)).map(propLine => Property(propLine._1, toHighLighter(propLine._2)))
      ExternalType(tupleOfVarLineType._2, (toHighLighter(List(tupleOfVarLineType._1.lineNumber))
        ::: propLines.toList.flatMap(_.lines).asInstanceOf[List[ExternalLine]]).distinct,
        propLines.toSet)
    }.filter(_.properties.nonEmpty)
  }

  /* Generates a token for each file by considering the
   whole file as a single block of context */
  def generateTypeReferences(fileNamesVsContent: Map[String, String],
                             repository: Option[Repository]): Set[ExternalTypeReference] = {

    val repo = repository.getOrElse(Repository.invalid)
    fileNamesVsContent.map { tupleOfNameContent =>
      val fileName = tupleOfNameContent._1
      log.info(s"Current fileName >>> $fileName")
      val irFactory = new IRFactory(env)
      val mayBeAstRoot = Try(irFactory.parse(tupleOfNameContent._2, "", 1))
      if (mayBeAstRoot.isSuccess) {
        val types = getTypes(mayBeAstRoot.get).toSet
        val gitFileName = SparkIndexJobHelper.fileNameToURL(repo, tupleOfNameContent._1)
        ExternalTypeReference(repo.id, gitFileName, types, repo.stargazersCount)
      } else {
        ExternalTypeReference(-1, "", Set[ExternalType](), -1)
      }
    }.filter(x => x.types.nonEmpty).toSet
  }
}
