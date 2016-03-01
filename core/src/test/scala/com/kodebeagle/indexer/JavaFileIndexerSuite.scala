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

import java.io.StringWriter

import org.apache.commons.io.IOUtils
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class JavaASTBasedIndexerForMethodsSuite extends FunSuite with BeforeAndAfterAll {
  val stream =
    Thread.currentThread().getContextClassLoader.getResourceAsStream("TransportClient.java")
  val writer = new StringWriter()
  val sampleRepo = RepoFileNameInfo("sample", 0, "sample", false, "Java", "master", 0, "")

  override def beforeAll() {
    IOUtils.copy(stream, writer)
  }

  test("Parse a file and verify method tokens") {
    val indexer = new JavaExternalTypeRefIndexer
    val methodTokens = indexer.generateTypeReferences(Map("sample-master/Sample.java" -> writer
      .toString), List(), Some(Repository.invalid))

    assert(methodTokens.size === 7)

    val importChannel: String = "io.netty.channel.Channel"
    val importObjects: String = "com.google.common.base.Objects"
    val importLogger: String = "org.slf4j.Logger"
    val importNettyUtils: String = "org.apache.spark.network.util.NettyUtils"
    val importIOE: String = "java.io.IOException"
    val importUUID: String = "java.util.UUID"
    val importTimeUnit: String = "java.util.concurrent.TimeUnit"
    val importRpcReq: String = "org.apache.spark.network.protocol.RpcRequest"
    val importEE: String = "java.util.concurrent.ExecutionException"
    val importThrowables: String = "com.google.common.base.Throwables"
    val importSFuture: String = "com.google.common.util.concurrent.SettableFuture"
    val importPrecon: String = "com.google.common.base.Preconditions"
    val importSCId: String = "org.apache.spark.network.protocol.StreamChunkId"
    val importCFReq: String = "org.apache.spark.network.protocol.ChunkFetchRequest"
    val importXMLAnnotation: String = "javax.xml.bind.annotation.XmlAnyAttribute"
    val sampleFile: String = "n-a/n-a/blob/n-a/Sample.java"

    val testMethodTokens = Set(ExternalTypeReference(-1, sampleFile,
      Set(ExternalType(importChannel,
        List(ExternalLine(204, 28, 34)), Set(Property("remoteAddress",
          List(ExternalLine(204, 28, 50))))),
        ExternalType(importObjects, List(ExternalLine(203, 12, 18))
          , Set(Property("toStringHelper", List(ExternalLine(203, 12, 39)))))), 0),

      ExternalTypeReference(-1, sampleFile, Set(ExternalType(importTimeUnit,
        List(ExternalLine(187, 36, 43)), Set()), ExternalType(importEE,
        List(ExternalLine(188, 7, 5), ExternalLine(189, 34, 34))
        , Set(Property("getCause", List(ExternalLine(189, 34, 45))))),
        ExternalType(importThrowables, List(ExternalLine(189, 13, 22),
          ExternalLine(191, 13, 22)), Set(Property("propagate", List(ExternalLine(189, 13, 46),
          ExternalLine(191, 13, 35))))), ExternalType(importSFuture, List(ExternalLine(182, 9, 14),
          ExternalLine(187, 14, 19), ExternalLine(172, 11, 32), ExternalLine(172, 43, 56),
          ExternalLine(177, 9, 14)), Set(Property("set", List(ExternalLine(177, 9, 28))),
          Property("get", List(ExternalLine(187, 14, 57))), Property("create",
            List(ExternalLine(172, 43, 65))), Property("setException",
            List(ExternalLine(182, 9, 30)))))), 0),

      ExternalTypeReference(-1, sampleFile, Set(ExternalType(importTimeUnit,
        List(ExternalLine(198, 46, 53)), Set()), ExternalType(importChannel,
        List(ExternalLine(198, 5, 11)), Set(Property("close",
          List(ExternalLine(198, 5, 19)))))), 0),

      ExternalTypeReference(-1, sampleFile, Set(ExternalType(importXMLAnnotation,
        List(ExternalLine(78, 4, 18)), Set()), ExternalType("io.netty.channel.Channel",
        List(ExternalLine(79, 12, 18), ExternalLine(79, 32, 38)), Set(Property("isOpen",
          List(ExternalLine(79, 12, 27))), Property("isActive",
          List(ExternalLine(79, 32, 49)))))), 0),

      ExternalTypeReference(-1, sampleFile, Set(ExternalType(importChannel,
        List(ExternalLine(74, 10, 16), ExternalLine(74, 47, 53)), Set()),
        ExternalType(importPrecon, List(ExternalLine(74, 20, 32), ExternalLine(75, 20, 32)),
        Set(Property("checkNotNull", List(ExternalLine(74, 20, 54), ExternalLine(75, 20, 54)))))),
        0),

      ExternalTypeReference(-1, sampleFile,
        Set(ExternalType(importCFReq, List(ExternalLine(108, 31, 47)), Set()),
          ExternalType(importChannel, List(ExternalLine(101, 59, 65), ExternalLine(121, 13, 19),
            ExternalLine(108, 5, 11)), Set(Property("close", List(ExternalLine(121, 13, 27))),
            Property("writeAndFlush", List(ExternalLine(108, 5, 63))))),
          ExternalType(importLogger, List(ExternalLine(103, 5, 10),
            ExternalLine(114, 13, 18), ExternalLine(119, 13, 18), ExternalLine(125, 15, 20)),
            Set(Property("trace", List(ExternalLine(114, 13, 24))), Property("debug",
              List(ExternalLine(103, 5, 80))), Property("error", List(ExternalLine(119, 13, 50),
              ExternalLine(125, 15, 85))))), ExternalType(importSCId,
            List(ExternalLine(120, 40, 52), ExternalLine(117, 84, 96), ExternalLine(106, 29, 41),
              ExternalLine(105, 11, 23), ExternalLine(108, 49, 61), ExternalLine(114, 65, 77),
              ExternalLine(105, 45, 57)), Set()), ExternalType(importChannel + "FutureListener",
            List(ExternalLine(109, 11, 31)), Set()), ExternalType(importNettyUtils,
            List(ExternalLine(101, 31, 40)), Set(Property("getRemoteAddress",
              List(ExternalLine(101, 31, 66))))),
          ExternalType(importIOE, List(ExternalLine(123, 50, 60)), Set()),
          ExternalType(importChannel + "Future", List(ExternalLine(112, 15, 20),
            ExternalLine(118, 27, 32), ExternalLine(119, 36, 41), ExternalLine(123, 72, 77)),
            Set(Property("cause", List(ExternalLine(118, 27, 40), ExternalLine(119, 36, 49),
              ExternalLine(123, 72, 85))), Property("isSuccess",
              List(ExternalLine(112, 15, 32)))))), 0), ExternalTypeReference(-1, sampleFile,
        Set(ExternalType(importUUID, List(ExternalLine(141, 37, 40)),
          Set(Property("randomUUID", List(ExternalLine(141, 37, 53))))),
          ExternalType(importIOE, List(ExternalLine(158, 38, 48)), Set()),
          ExternalType(importChannel, List(ExternalLine(137, 59, 65),
            ExternalLine(156, 13, 19), ExternalLine(144, 5, 11)), Set(Property("close",
            List(ExternalLine(156, 13, 27))), Property("writeAndFlush",
            List(ExternalLine(144, 5, 61))))),
          ExternalType(importNettyUtils, List(ExternalLine(137, 31, 40)),
            Set(Property("getRemoteAddress", List(ExternalLine(137, 31, 66))))),
          ExternalType(importLogger, List(ExternalLine(139, 5, 10),
            ExternalLine(150, 13, 18), ExternalLine(154, 13, 18), ExternalLine(160, 15, 20)),
            Set(Property("trace", List(ExternalLine(139, 5, 49), ExternalLine(150, 13, 97))),
              Property("error", List(ExternalLine(154, 13, 50), ExternalLine(160, 15, 85))))),
          ExternalType(importRpcReq, List(ExternalLine(144, 31, 40)), Set()),
          ExternalType(importChannel + "Future", List(ExternalLine(148, 15, 20),
            ExternalLine(153, 27, 32), ExternalLine(154, 36, 41), ExternalLine(158, 60, 65)),
            Set(Property("cause", List(ExternalLine(153, 27, 40), ExternalLine(154, 36, 49),
              ExternalLine(158, 60, 73))), Property("isSuccess",
              List(ExternalLine(148, 15, 32))))), ExternalType(importChannel + "FutureListener",
            List(ExternalLine(145, 11, 31)), Set())), 0)
    )

    assert(methodTokens === testMethodTokens)
  }
}
