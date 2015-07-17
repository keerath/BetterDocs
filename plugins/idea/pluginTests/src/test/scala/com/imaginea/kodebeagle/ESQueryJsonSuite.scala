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

package com.imaginea.kodebeagle

import com.imaginea.kodebeagle.`object`.WindowObjects
import com.imaginea.kodebeagle.util.JSONUtils
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import scala.collection.JavaConversions._

class ESQueryJsonSuite extends FunSuite with BeforeAndAfterAll{

  private val windowObjects = WindowObjects.getInstance()
  windowObjects.setEsURL("http://labs.imaginea.com/kodebeagle")

  test("actualJSON should match with expectedJSON") {
    val imortsSet = Set("java.util.Map")
    val size = 10
    val actualJSON = new JSONUtils().getESQueryJson(imortsSet, size)
    val expectedJSON = "{\"query\":{\"bool\":{\"must\":[" +
      "{\"term\":{\"typeimportsmethods.tokens.importName\":\"java.util.map\"}}]," +
      "\"mustNot\":[],\"should\":[]}},\"from\":0,\"size\":10," +
      "\"sort\":[{\"score\":{\"order\":\"desc\"}}]}"

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
