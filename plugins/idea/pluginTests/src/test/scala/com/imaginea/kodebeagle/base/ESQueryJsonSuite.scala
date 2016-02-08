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

package com.imaginea.kodebeagle.base

import com.imaginea.kodebeagle.base.`object`.WindowObjects
import com.imaginea.kodebeagle.base.util.JSONUtils
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

class ESQueryJsonSuite extends FunSuite with BeforeAndAfterAll {

  private val windowObjects = WindowObjects.getInstance()
  windowObjects.setEsURL("http://labs.imaginea.com/kodebeagle")

  test("Testing EsQueryJson for Java imports with methods") {
    val importVsMethods = Map("java.util.List" -> Set("add").asJava,
      "java.util.HashMap" -> Set("put").asJava)
    val size = 30
    val includeMethods = true
    val actualJSON = new JSONUtils().getESQueryJson(importVsMethods.asJava, size,
      includeMethods)
    val expectedJSON: String =
      """{"query":{"filtered":{"filter":{"and":[{"nested":{"path":"tokens","filter":{"bool":
        |{"must":[{"term":{"tokens.importName":"java.util.list"}}],"should":[{"terms":
        |{"tokens.methodAndLineNumbers.methodName":["add"]}}]}}}},{"nested":{"path":
        |"tokens","filter":{"bool":{"must":[{"term":{"tokens.importName":
        |"java.util.hashmap"}}],"should":[{"terms":{"tokens.methodAndLineNumbers.
        |methodName":["put"]}}]}}}}]},"_cache":true}},"from":0,"size":30,"sort":
        |[{"score":{"order":"desc"}}]}""".stripMargin.replaceAll("\n", "").replaceAll("\r\n", "")

    assert(actualJSON == expectedJSON)
  }

  test("Tesing EsQueryJson for Scala imports") {
    val importsVsMethods = Map("akka.actor.actorref" -> Set[String]().asJava,
      "akka.actor.props" -> Set[String]().asJava)
    val size = 30
    val includeMethods = false
    val actualJSON = new JSONUtils().getESQueryJson(importsVsMethods.asJava, size,
      includeMethods)
    val expectedJSON =
      """{"query":{"filtered":{"filter":{"and":[{"term":{"tokens.importName":"akka.actor.actorref"
        |}},{"term":{"tokens.importName":"akka.actor.props"}}]},"_cache":true}},"from":0,"size":30,
        |"sort":[{"score":{"order":"desc"}}]}""".stripMargin.replaceAll("\n", "")
        .replaceAll("\r\n", "")
    assert(actualJSON == expectedJSON)
  }

  test("Testing EsQueryJson for Java imports only") {
    val emptyMethods: Set[String] = Set()
    val importVsMethods = Map("java.util.List" -> emptyMethods.asJava,
      "java.util.HashMap" -> emptyMethods.asJava)
    val size = 30
    val includeMethods = false
    val actualJSON = new JSONUtils().getESQueryJson(importVsMethods.asJava, size,
      includeMethods)
    val expectedJSON: String =
      """{"query":{"filtered":{"filter":{"and":[{"term":{"tokens.importName":"java.util.list"}}
        |,{"term":{"tokens.importName":"java.util.hashmap"}}]},"_cache":true}},"from":0,"size":30,
        |"sort":[{"score":{"order":"desc"}}]}""".stripMargin.replaceAll("\n", "")
        .replaceAll("\r\n", "")
    assert(actualJSON == expectedJSON)
  }

  private val fileName = "apache/ofbiz/blob/trunk/framework/" +
    "entity/src/org/ofbiz/entity/model/ModelEntity.java"

  ignore("actual file content json should match with expectedJSON") {
    val expectedFileContentJSON = new JSONUtils().getJsonForFileContent(List(fileName))
    val actualFileContentJSON = "{\"query\":{\"term\":{\"fileName\":\"" +
      "apache/ofbiz/blob/trunk/framework/entity/src/org/ofbiz/entity/model/ModelEntity.java\"}}}"

    assert(expectedFileContentJSON == actualFileContentJSON)
  }
}
