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

import com.kodebeagle.indexer.{Repository, ExternalLine, ExternalType, JavaExternalTypeRefIndexer, Property, RepoFileNameInfo}
import org.apache.commons.io.IOUtils
import org.mozilla.javascript.ast.ErrorCollector
import org.mozilla.javascript.{CompilerEnvirons, IRFactory}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.util.Try

class ParserSuite extends FunSuite with BeforeAndAfterAll {

  test("Simple repo") {
    val r = RepoFileNameParser("repo~apache~zookeeper~160999~false~Java~trunk~789.zip")
    assert(r.get == RepoFileNameInfo("apache", 160999, "zookeeper", false, "Java", "trunk", 789, ""))
  }

  test("Names with special character") {
    val r = RepoFileNameParser("/home/dir~temp/repo~apache~zookeeper-lost~160999~false~Java" +
      "~=+-trunk/2.1~789.zip")
    assert(r.get == RepoFileNameInfo("apache", 160999, "zookeeper-lost", false,
      "Java", "=+-trunk/2.1", 789, ""))
  }

  test("Branch name with version number only") {
    val r = RepoFileNameParser("repo~apache~zookeeper~160999~false~Java~2.1~789.zip")
    assert(r.get == RepoFileNameInfo("apache", 160999, "zookeeper", false, "Java", "2.1", 789, ""))
  }


  test("Branch name with tilde in it.") {
    val r = RepoFileNameParser("repo~apache~zookee~per~160999~false~Java~2.~1~789.zip")
    assert(r.isEmpty)
  }

  test("Branch name absent.") {
    val r = RepoFileNameParser("repo~apache~zookeeper~160999~false~Java~789.zip")
    assert(r.get == RepoFileNameInfo("apache", 160999, "zookeeper", false, "Java", "master", 789, ""))
  }

  test("Hdfs url.") {
    val r = RepoFileNameParser(
      "/172.16.13.179:9000/user/data/github3/repo~apache~zookeeper~160999~false~Java~789.zip")
    assert(r.get == RepoFileNameInfo("apache", 160999, "zookeeper", false, "Java", "master", 789, ""))
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

class MethodVisitorSuite extends FunSuite with BeforeAndAfterAll {

  import scala.collection.JavaConversions._

  val stream: InputStream =
    Thread.currentThread.getContextClassLoader.getResourceAsStream("TransportClient.java")

  val parser: MethodVisitor = new MethodVisitor
  parser.parse(stream)

  test("Verify method visitor includes lines with usages.") {

    val lines = parser.getListOflineNumbersMap.flatMap(x => x.flatMap(_._2)).map(_.toInt)
      .toList.sorted.distinct

    assert(lines === List(74, 75, 78, 79, 101, 102, 103, 105, 106, 108, 109, 112, 113, 114, 115,
      117, 118, 119, 120, 121, 123, 124, 125, 137, 138, 139, 141, 142, 144, 145, 148, 149, 150, 152,
      153, 154, 155, 156, 158, 159, 160, 172, 174, 177, 182, 187, 188, 189, 190, 191, 198, 203,
      204))
  }

  test("Should return a Map of import with methods and lineNumbers") {
    import java.io.InputStream
    val stream: InputStream =
      Thread.currentThread.getContextClassLoader.getResourceAsStream("TransportClient.java")

    import com.kodebeagle.parser.MethodVisitorHelper._

    val testMap = List(Map("io.netty.channel.Channel" -> Map(),
      "com.google.common.base.Preconditions" -> Map("checkNotNull" ->
        List(ExternalLine(74, 20, 54), ExternalLine(75, 20, 54)))),

      Map("io.netty.channel.Channel" -> Map("isOpen" -> List(ExternalLine(79, 12, 27)),
        "isActive" -> List(ExternalLine(79, 32, 49))),
        "javax.xml.bind.annotation.XmlAnyAttribute" -> Map()),

      Map("org.slf4j.Logger" -> Map("trace" -> List(ExternalLine(114, 13, 24)),
        "debug" -> List(ExternalLine(103, 5, 80)),
        "error" -> List(ExternalLine(119, 13, 50), ExternalLine(125, 15, 85))),
        "org.apache.spark.network.protocol.ChunkFetchRequest" -> Map(),
        "io.netty.channel.ChannelFutureListener" -> Map(), "java.io.IOException" -> Map(),
        "org.apache.spark.network.util.NettyUtils" -> Map("getRemoteAddress" ->
          List(ExternalLine(101, 31, 66))), "io.netty.channel.ChannelFuture" ->
          Map("cause" -> List(ExternalLine(118, 27, 40), ExternalLine(119, 36, 49),
            ExternalLine(123, 72, 85)), "isSuccess" -> List(ExternalLine(112, 15, 32))),
        "io.netty.channel.Channel" -> Map("close" -> List(ExternalLine(121, 13, 27)),
          "writeAndFlush" -> List(ExternalLine(108, 5, 63))),
        "org.apache.spark.network.protocol.StreamChunkId" -> Map()),

      Map("org.slf4j.Logger" -> Map("trace" -> List(ExternalLine(139, 5, 49),
        ExternalLine(150, 13, 97)),
        "error" -> List(ExternalLine(154, 13, 50), ExternalLine(160, 15, 85))),
        "io.netty.channel.ChannelFutureListener" -> Map(),
        "java.io.IOException" -> Map(), "java.util.UUID" ->
          Map("randomUUID" -> List(ExternalLine(141, 37, 53))),
        "org.apache.spark.network.util.NettyUtils" ->
          Map("getRemoteAddress" -> List(ExternalLine(137, 31, 66))),
        "io.netty.channel.ChannelFuture" -> Map("cause" -> List(ExternalLine(153, 27, 40),
          ExternalLine(154, 36, 49), ExternalLine(158, 60, 73)), "isSuccess" ->
          List(ExternalLine(148, 15, 32))), "io.netty.channel.Channel" -> Map("close" ->
          List(ExternalLine(156, 13, 27)), "writeAndFlush" -> List(ExternalLine(144, 5, 61))),
        "org.apache.spark.network.protocol.RpcRequest" -> Map()),

      Map("java.util.concurrent.TimeUnit" -> Map(), "java.util.concurrent.ExecutionException" ->
        Map("getCause" -> List(ExternalLine(189, 34, 45))), "com.google.common.base.Throwables" ->
        Map("propagate" -> List(ExternalLine(189, 13, 46), ExternalLine(191, 13, 35))),
        "com.google.common.util.concurrent.SettableFuture" -> Map("set" ->
          List(ExternalLine(177, 9, 28)), "get" -> List(ExternalLine(187, 14, 57)),
          "create" -> List(ExternalLine(172, 43, 65)),
          "setException" -> List(ExternalLine(182, 9, 30)))),

      Map("java.util.concurrent.TimeUnit" -> Map(), "io.netty.channel.Channel" ->
        Map("close" -> List(ExternalLine(198, 5, 19)))),

      Map("io.netty.channel.Channel" -> Map("remoteAddress" -> List(ExternalLine(204, 28, 50))),
        "com.google.common.base.Objects" ->
          Map("toStringHelper" -> List(ExternalLine(203, 12, 39))))
    )
    val javaASTBasedIndexer = new JavaExternalTypeRefIndexer()
    val imports = javaASTBasedIndexer.getImports(parser, Set())
      .map(importName => importName._1 + "." + importName._2)
    val tokenMap: List[Map[String, List[ExternalLine]]] = getTokenMap(parser, imports)
    val resultTokens = getImportsWithMethodAndLineNumbers(parser, tokenMap).map(_._1)

    for (i <- resultTokens.indices) {
      val resultToken = resultTokens(i)
      val testToken = testMap(i)
      assert(resultToken.size === testToken.size)
      assert((resultToken.keySet diff testToken.keySet) === Set())
      assert((testToken.keySet diff resultToken.keySet) === Set())
      resultToken.foreach {
        case (k, v) =>
          assert(resultToken(k) === testToken(k))
      }
    }
  }

  override def afterAll() {
    stream.close()
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

class ZipParserTest extends FunSuite {

  import java.util.zip.ZipInputStream

  import com.kodebeagle.crawler.ZipBasicParser._

  test("read zip file correctly") {
    val zipName = "repo~Cascading~cascading-dbmigrate~576623~false~Java~master~65.zip"
    val stream: InputStream = Thread.currentThread.getContextClassLoader.getResourceAsStream(
      zipName)
    val repoFileNameInfo = RepoFileNameParser(zipName)
    val (_, _, _, repository) = readFilesAndPackages(repoFileNameInfo, new ZipInputStream(stream))
    assert(repository.get == Repository("Cascading", 576623, "cascading-dbmigrate",
      false, "Java", "master", 65, 843, 8, 33, ""))
  }
}
