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

import scala.collection.mutable.ListBuffer
import scala.util.Try
import scalariform.lexer.{Token, Tokens}
import scalariform.parser.{AstNode, CallExpr, FunDefOrDcl, Param, ParamClause}
import scalariform.utils.Range

class ScalaParserBase(funDefOrDcl: FunDefOrDcl) {
  def getUsageRanges(param: String): List[Range] = {
    funDefOrDcl.tokens.filter(_.rawText == param).map(_.range)
  }

  def getCallExprAndRanges(callExprs: List[CallExpr],
                           param: String): Map[String, List[Range]] = {
    val matchedCallExprs = callExprs.filter(callExpr =>
      callExpr.tokens.map(_.rawText).contains(param))
    val methodCallTokens = matchedCallExprs.flatMap(callExpr =>
      getMethodCallTokens(callExpr.tokens.toArray, param))
    methodCallTokens.map(methodCallToken => (methodCallToken.rawText, methodCallToken))
      .groupBy(_._1).mapValues(_.map(x => x._2.range)).filter(_._2.nonEmpty)
  }

  def getAllCallExprs: List[CallExpr] = {
    val buff = ListBuffer[CallExpr]()
    def recursiveVisit(children: List[AstNode]): Unit = {
      children.map {
        case callExpr: CallExpr => buff += callExpr
        case other => if (other.immediateChildren.nonEmpty) {
          recursiveVisit(other.immediateChildren)
        } else {
          recursiveVisit(List[AstNode]())
        }
      }
    }
    funDefOrDcl.funBodyOpt match {
      case Some(x) => recursiveVisit(x.immediateChildren)
      case None =>
    }
    buff.toList
  }

  def getMethodCallTokens(tokens: Array[Token], param: String): Array[Token] = {
    val optionTupleOfTokens = tokens.zipWithIndex.flatMap(x => x._1.rawText == "." match {
      case true => Some(Try(tokens(x._2 - 1)).toOption, Try(tokens(x._2 + 1)).toOption)
      case false => None
    })
    val identityToken = Token(Tokens.VARID, "", -1, "")
    val tupleOfTokens = optionTupleOfTokens.map(x => (x._1.getOrElse(identityToken),
      x._2.getOrElse(identityToken)))
    tupleOfTokens.filter(_._1.rawText == param).map(_._2)
  }

  def getListOfParamVsType: List[(String, String)] = {
    def getAllParams(paramClause: ParamClause) = {
      val mutableList = ListBuffer[Param]()
      paramClause.firstParamOption match {
        case Some(param) => mutableList += param
        case None =>
      }
      mutableList ++= paramClause.otherParams.map(_._2)
      mutableList.toList
    }

    funDefOrDcl.paramClauses.paramClausesAndNewlines.flatMap(x => {
      val allParams = getAllParams(x._1)
      allParams.map(x => (x.id.rawText, x.paramTypeOpt.get._2.tokens
        .filter(_.tokenType == Tokens.VARID).map(_.rawText).headOption match {
        case Some(text) => text
        case None => ""
      })).filter(_._2.nonEmpty)
    })
  }
}
