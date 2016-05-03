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

package com.kodebeagle.integeration

import org.apache.commons.httpclient.{NameValuePair, HttpClient}
import org.apache.commons.httpclient.methods.GetMethod

import org.json4s._
import org.json4s.jackson.JsonMethods._

object SearchTest extends App {

  performTests()

  private def performTests() = {
    testAsyncFCByteBuff() // AsyncFileChannel & ByteBuffer
    testDateSimpleDateFormat() // Date & SimpleDateFormat
    testBuffReaderFileReader() // BufferedReader & FileReader
    testSocketDIS() // Socket & DataInputStream
    testMatcherPattern() // Matcher & Pattern
  }

  private def testAsyncFCByteBuff() = {
    val esQuery =
      """{"query":{"bool":{"must":[{"wildcard":
        |{"tokens.importName":"*asynchronousfilechannel"}},{"wildcard":
        |{"tokens.importName":"*bytebuffer"}}],"must_not":[],"should":[]}}
        |,"sort":[{"score":{"order":"desc"}}]}""".stripMargin

    val topHits = extractTopThreeHits(doRequest(esQuery))
    val expectedTopHits = List(
      "eclipse/vert.x/blob/master/src/main/java/io/vertx/core/file/impl/AsyncFileImpl.java",
      "eclipse/vert.x/blob/master/src/main/java/io/vertx/core/file/impl/AsyncFileImpl.java",
      "apache/lucene-solr/blob/trunk/lucene/test-framework/src/java/org/apache/lucene" +
        "/mockfile/FilterAsynchronousFileChannel.java")

    assert(topHits.forall(expectedTopHits.contains(_)))
  }

  private def testDateSimpleDateFormat() = {
    val esQuery =
      """{"query":{"bool":{"must":[{"wildcard":{"tokens.importName":"*date"}}
        |,{"wildcard":{"tokens.importName":"*simpledateformat"}}],"must_not"
        |:[],"should":[]}},"sort":[{"score":{"order":"desc"}}]}""".stripMargin

    val topHits = extractTopThreeHits(doRequest(esQuery))

    val expectedTopHits = List(
      "google/iosched/blob/master/updater/src/main/java/com/google/iosched/model/" +
        "validator/DateTimeConverter.java",
      "google/iosched/blob/master/android/src/main/java/com/google/samples/apps/" +
        "iosched/util/UIUtils.java",
      "google/iosched/blob/master/updater/src/main/java/com/google/iosched/model/DataCheck.java"
    )

    assert(topHits.forall(expectedTopHits.contains(_)))
  }

  private def testBuffReaderFileReader() = {
    val esQuery =
      """{"query":{"bool":{"must":[{"wildcard":{"tokens.importName":"*bufferedreader"}}
        |,{"wildcard":{"tokens.importName" :"*filereader"}}],"must_not":[],"should":[]}}
        |,"sort":[{"score":{"order":"desc"}}]}""".stripMargin

    val topHits = extractTopThreeHits(doRequest(esQuery))

    val expectedTopHits = List(
      "PredictionIO/PredictionIO/blob/develop/examples/experimental/" +
        "java-local-helloworld/MyDataSource.java",
      "PredictionIO/PredictionIO/blob/develop/examples/experimental/" +
        "java-local-tutorial/src/main/java/recommendations/tutorial4/DataSource.java",
      "iluwatar/java-design-patterns/blob/master/model-view-presenter" +
        "/src/main/java/com/iluwatar/model/view/presenter/FileLoader.java"
    )

    assert(topHits.forall(expectedTopHits.contains(_)))
  }

  private def testSocketDIS() = {
    val esQuery =
      """{"query":{"bool":{"must":[{"wildcard":{"tokens.importName":"*socket"}},
        |{"wildcard":{"tokens.importName":"*datainputstream"}}],"must_not":[],
        |"should":[]}},"sort":[{"score":{"order":"desc"}}]}""".stripMargin

    val topHits = extractTopThreeHits(doRequest(esQuery))

    val expectedTopHits = List(
      "libgdx/libgdx/blob/master/gdx/src/com/badlogic/gdx/input/RemoteInput.java",
      "android/platform_frameworks_base/blob/master/core/java/android/os/Process.java",
      "android/platform_frameworks_base/blob/master/core/java/android/os/Process.java"
    )

    assert(topHits.forall(expectedTopHits.contains(_)))
  }

  private def testMatcherPattern() = {
    val esQuery =
      """{"query":{"bool":{"must":[{"wildcard":{"tokens.importName":"*matcher"}}
        |,{"wildcard":{"tokens.importName":"*pattern"}}],"must_not":[],"should":[]}}
        |,"sort":[{"score" :{"order":"desc"}}]}""".stripMargin

    val topHits = extractTopThreeHits(doRequest(esQuery))

    val expectedTopHits = List(
      "elastic/elasticsearch/blob/master/src/main/java/org/elasticsearch/" +
        "gateway/MetaDataStateFormat.java",
      "elastic/elasticsearch/blob/master/src/main/java/org/elasticsearch/" +
        "search/aggregations/AggregatorParsers.java",
      "elastic/elasticsearch/blob/master/src/main/java/org/elasticsearch/" +
        "common/settings/ImmutableSettings.java"
    )

    assert(topHits.forall(expectedTopHits.contains(_)))
  }

  private def doRequest(esQuery: String) = {
    val httpClient = new HttpClient()
    val getMethod = new GetMethod("http://labs.imaginea.com")
    getMethod.setPath("/kodebeagle/importsmethods/typeimportsmethods/_search")
    getMethod.setQueryString(Array(new NameValuePair("source", esQuery)))
    httpClient.executeMethod(getMethod)
    getMethod.getResponseBodyAsString
  }

  private def extractTopThreeHits(response: String) = {
    val jsonObj = parse(response)
    val hits = (jsonObj \ "hits" \ "hits").asInstanceOf[JArray].arr
    hits.take(3)
      .map(x => (x \ "_source" \ "file").asInstanceOf[JString].values)
  }
}
