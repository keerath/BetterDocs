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

import java.io.{FileReader, InputStream, StringWriter}

import com.kodebeagle.indexer.{ExternalLine, ExternalType, Property, RepoFileNameInfo}
import com.kodebeagle.util.RepoFileNameParser
import org.apache.commons.io.IOUtils
import org.mozilla.javascript.ast.ErrorCollector
import org.mozilla.javascript.{CompilerEnvirons, IRFactory}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.util.Try

class ParserSuite extends FunSuite with BeforeAndAfterAll {

  test("Simple repo") {
    val r = RepoFileNameParser("repo~apache~zookeeper~160999~false~Java~trunk~789.zip")
    assert(r.get == RepoFileNameInfo("apache", 160999, "zookeeper", false, "Java", "trunk", 789))
  }

  test("Names with special character") {
    val r = RepoFileNameParser("/home/dir~temp/repo~apache~zookeeper-lost~160999~false~Java" +
      "~=+-trunk/2.1~789.zip")
    assert(r.get == RepoFileNameInfo("apache", 160999, "zookeeper-lost", false,
      "Java", "=+-trunk/2.1", 789))
  }

  test("Branch name with version number only") {
    val r = RepoFileNameParser("repo~apache~zookeeper~160999~false~Java~2.1~789.zip")
    assert(r.get == RepoFileNameInfo("apache", 160999, "zookeeper", false, "Java", "2.1", 789))
  }


  test("Branch name with tilde in it.") {
    val r = RepoFileNameParser("repo~apache~zookee~per~160999~false~Java~2.~1~789.zip")
    assert(r.isEmpty)
  }

  test("Branch name absent.") {
    val r = RepoFileNameParser("repo~apache~zookeeper~160999~false~Java~789.zip")
    assert(r.get == RepoFileNameInfo("apache", 160999, "zookeeper", false, "Java", "master", 789))
  }

  test("Hdfs url.") {
    val r = RepoFileNameParser(
      "/172.16.13.179:9000/user/data/github3/repo~apache~zookeeper~160999~false~Java~789.zip")
    assert(r.get == RepoFileNameInfo("apache", 160999, "zookeeper", false, "Java", "master", 789))
  }

  test("Multiple valid repo names.") {
    val stream =
      Thread.currentThread().getContextClassLoader.getResourceAsStream("repo_names")
    val writer = new StringWriter()
    IOUtils.copy(stream, writer)
    val repoNames = writer.toString.split("\n").map { x =>
      (RepoFileNameParser(x), x)
    }
    assert(repoNames.filter(p = x => x._1.isEmpty) === Seq())
  }
}

class JsParserTest extends FunSuite {
  val location = Thread.currentThread().getContextClassLoader.getResource("Test.js")
  val reader = new FileReader(location.getFile)
  val env = new CompilerEnvirons()
  env.setRecoverFromErrors(true)
  env.setIdeMode(true)
  env.setErrorReporter(new ErrorCollector)
  env.setStrictMode(false)
  val irFactory = new IRFactory(env)
  val mayBeAstRoot = Try(irFactory.parse(reader, "", 1))
  test("Properties of a particular javascript object") {
    if (mayBeAstRoot.isSuccess) {
      // Getting properties for javascript object Rx
      val actualProps = JsParser.getProperties(mayBeAstRoot.get, "Rx").map(_.toSource).distinct
      val expectedProps = List("config", "Disposable", "Observable")
      assert(actualProps.forall(x => expectedProps.contains(x)))
    }
  }

  test("Types and their associated properties in a js file") {
    if (mayBeAstRoot.isSuccess) {
      val actualTypes = JsParser.getTypes(mayBeAstRoot.get).toSet
      val expectedTypes =
        Set(ExternalType("rx", List(ExternalLine(4, -1, -1), ExternalLine(11, -1, -1),
          ExternalLine(87, -1, -1), ExternalLine(173, -1, -1), ExternalLine(45, -1, -1),
          ExternalLine(31, -1, -1), ExternalLine(70, -1, -1), ExternalLine(62, -1, -1),
          ExternalLine(78, -1, -1)), Set(Property("config", List(ExternalLine(11, -1, -1))),
          Property("Observable", List(ExternalLine(87, -1, -1), ExternalLine(173, -1, -1),
            ExternalLine(45, -1, -1), ExternalLine(31, -1, -1), ExternalLine(70, -1, -1))),
          Property("Disposable", List(ExternalLine(62, -1, -1), ExternalLine(78, -1, -1))))),
          ExternalType("mongodb.MongoClient",
            List(ExternalLine(10, -1, -1), ExternalLine(33, -1, -1)),
            Set(Property("connect", List(ExternalLine(33, -1, -1))))), ExternalType("mongodb",
            List(ExternalLine(7, -1, -1), ExternalLine(10, -1, -1)), Set(Property("MongoClient",
              List(ExternalLine(10, -1, -1))))))
      assert(actualTypes.exists(actualType => expectedTypes.exists(
        expectedType => actualType.typeName == expectedType.typeName &&
          actualType.lines.forall(expectedType.lines.contains(_)) &&
          actualType.properties.exists(actualProp => expectedType.properties.exists(
            expectedProp => actualProp.propertyName == expectedProp.propertyName &&
              actualProp.lines.forall(expectedProp.lines.contains(_)))))))
    }
  }

}

class ScalaParserTest extends FunSuite {

  val stream: InputStream = Thread.currentThread().getContextClassLoader
    .getResourceAsStream("PartioningUtils.scala")
  val source = scala.io.Source.fromInputStream(stream).mkString
  test("extract functions from scala file") {
    val actualFunctions = ScalaParser.extractFunctions(source).map(_.nameToken.rawText)
    val expectedFunctions = List("parsePartitions",
      "parsePartition", "parsePartitionColumn", "resolvePartitions",
      "listConflictingPartitionColumns", "inferPartitionColumnValue",
      "validatePartitionColumnDataTypes", "resolveTypeConflicts", "needsEscaping",
      "escapePathName", "unescapePathName")
    assert(actualFunctions == expectedFunctions)
  }

  val testFunction = ScalaParser.extractFunctions(source).head
  val scalaFuncHelper = new ScalaParserBase(testFunction)
  test("params for a function") {
    val actualParams = scalaFuncHelper.getListOfParamVsType
    val expectedParams = List(("paths", "Seq"), ("defaultPartitionName", "String"),
      ("typeInference", "Boolean"), ("basePaths", "Set"))
    assert(actualParams == expectedParams)
  }

  val allMethodCallExprs = scalaFuncHelper.getAllCallExprs
  test("All Method Call Exprs inside a function") {
    val expectedCallExprs = List("unzip", "flatMap", "isEmpty", "emptySpec", "assert",
      "resolvePartitions", "head", "map", "PartitionSpec")
    assert(allMethodCallExprs.map(_.id.rawText).distinct == expectedCallExprs)
  }

  test("Method Call Expr for a particular param") {
    val actualCallExprs = scalaFuncHelper.getCallExprAndRanges(allMethodCallExprs, "paths")
      .keys.toList
    val expectedCallExprs = List("zip", "map")
    assert(actualCallExprs == expectedCallExprs)
  }

  import scalariform.utils.Range

  test("Highlighters of usage for a particular param") {
    val actualHighlighters = scalaFuncHelper.getUsageRanges("paths")

    val expectedHighlighters = List(Range(2993, 5), Range(3361, 5), Range(3645, 5))
    assert(actualHighlighters == expectedHighlighters)
  }

  test("Highlighters of methodcall for a particular param") {
    val actualCallExprAndLines = scalaFuncHelper.getCallExprAndRanges(allMethodCallExprs, "paths")
    val expectedCallExprAndLines = Map("zip" -> List(Range(3651, 3)), "map" -> List(Range(3367, 3)))
    assert(actualCallExprAndLines == expectedCallExprAndLines)
  }
}
