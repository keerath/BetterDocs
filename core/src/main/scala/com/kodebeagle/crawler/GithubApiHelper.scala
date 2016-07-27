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

package com.kodebeagle.crawler

import com.kodebeagle.configuration.KodeBeagleConfig
import com.kodebeagle.logging.Logger
import org.apache.commons.httpclient.methods.GetMethod
import org.apache.commons.httpclient.params.HttpConnectionManagerParams
import org.apache.commons.httpclient.{HttpClient, MultiThreadedHttpConnectionManager, SimpleHttpConnectionManager}
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer
import scala.util.Try

/**
  * This class relies on Github's {https://developer.github.com/v3/} Api.
  */

case class Token(index: Int, value: String, var tried: Boolean)

object GithubApiHelper extends Logger with Serializable {

  implicit val format = DefaultFormats

  val tokens = ArrayBuffer.empty[Token] ++ KodeBeagleConfig.githubTokens.zipWithIndex
    .map(tokenIndex => Token(tokenIndex._2, tokenIndex._1, tried = false))

  val maxRetries = KodeBeagleConfig.httpClientMaxRetries

  /**
    * Access Github's
    * [[https://developer.github.com/v3/repos/#list-all-public-repositories List all repositories]]
    *
    * @param start Specify id of repo to start the listing from. (Pagination)
    * @param end   Specify the end to filter repos with in this range.
    */

  def queryRepoNamesWithInRange(start: Int, end: Long): List[String] = {

    def getRepoNames(mayBeJsonArr: Option[JValue]): Option[List[String]] = {
      mayBeJsonArr.map(_.children.map(json => Map("id" -> (json \ "id").extract[String],
        "fork" -> (json \ "fork").extract[Boolean].toString,
        "full_name" -> (json \ "full_name").extract[String]))
        .filter(x => x("id").toInt <= (end + 1) && !x("fork").toBoolean)
        .map(_ ("full_name")))
    }

    @tailrec
    def queryRepoNamesWithInRangeAccum(start: Long, end: Long,
                                       accum: List[String]): List[String] = {
      if (start <= end) {
        val endPoint = s"https://api.github.com/repositories?since=$start"
        val (mayBeJsonArr, mayBeNextSince) = queryRateLimitAPI(endPoint)
        getRepoNames(mayBeJsonArr) match {
          case Some(repoNames) =>
            queryRepoNamesWithInRangeAccum(mayBeNextSince.get, end, accum ++ repoNames)
          case None => accum
        }
      } else {
        accum
      }
    }
    queryRepoNamesWithInRangeAccum(start, end, List[String]())
  }

  def queryRepoMetadata(repoName: String): Option[String] =
    for {
      mayBeRepo <- queryRateLimitAPI("https://api.github.com/repos/" + repoName)._1
    } yield compact(render(mayBeRepo))


  @tailrec
  private def queryRateLimitAPI(query: String): (Option[JValue], Option[Long]) = {

    def updateTokens(): Unit = {
      val updatedTokens = tokens.map(token =>
        Token(token.index, token.value, tried = false))
      tokens.clear()
      tokens ++= updatedTokens
    }

    def getNextToken(tokens: ArrayBuffer[Token]) = tokens.find(!_.tried)

    getNextToken(tokens) match {

      case Some(token) =>
        executeRequest(query, token.value) match {
          case Some(method) =>
            val statusCode = method.getStatusCode
            val mayBeNextSince = Option(method.getResponseHeader("Link"))
              .map(_.getElements.toList.head.getValue.stripSuffix(">").trim.toLong)
            val limitRemaining =
              Option(method.getResponseHeader("X-RateLimit-Remaining")).getOrElse(0)
            if (statusCode == 403 && limitRemaining == 0) {
              log.info(s"Rate limit for token ${token.value} expired")
              tokens.update(token.index,
                Token(token.index, token.value, tried = true))
              queryRateLimitAPI(query)
            } else {
              val returnValue = (httpGetJson(method), mayBeNextSince)
              returnValue
            }
          case None => (None, None)
        }

      case None =>
        log.warn("Rate limit for all tokens is expired. Going to sleep")
        Thread.sleep(60 * 1000 * 3)
        updateTokens()
        queryRateLimitAPI(query)
    }
  }

  private def httpGetJson(method: GetMethod): Option[JValue] = {
    val status = method.getStatusCode
    if (status == 200) {
      // ignored parsing errors if any, because we can not do anything about them anyway.
      Try(parse(method.getResponseBodyAsString)).toOption
    } else {
      log.error("Request failed with status:" + status + "Response:"
        + method.getResponseHeaders.mkString("\n") +
        "\nResponseBody " + method.getResponseBodyAsString)
      None
    }
  }

  /**
    * Helper for accessing Java - Apache Http client.
    * (It it important to stick with the current version and all.)
    */

  def executeRequest(url: String, token: String): Option[GetMethod] = {

    def retryRequest(url: String, token: String, tries: Int): Option[GetMethod] = {
      val start = System.currentTimeMillis()
      val client = new HttpClient(getConnectionManager)
      val method = createGetMethod(url, token)
      val mayBeMethod = Try(client.executeMethod(method))
      if (mayBeMethod.isSuccess) {
        log.info(s"Request to $url took ${System.currentTimeMillis() - start}")
        Some(method)
      } else {
        if (tries >= 0) {
          log.info(s"Retrying request to $url")
          retryRequest(url, token, tries - 1)
        } else {
          log.warn(s"Skipping request to $url after $maxRetries")
          None
        }
      }

    }
    retryRequest(url, token, maxRetries)
  }

  private def createGetMethod(url: String, token: String): GetMethod = {
    val method = new GetMethod(url)
    method.setDoAuthentication(true)
    method.addRequestHeader("Authorization", s"token $token")
    method
  }

  private def getConnectionManager = {
    val httpConnectionManager = new SimpleHttpConnectionManager()

    val managerParams = new HttpConnectionManagerParams
    managerParams.setConnectionTimeout(10 * 1000)

    httpConnectionManager.setParams(managerParams)
    httpConnectionManager
  }
}
