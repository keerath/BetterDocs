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

import com.kodebeagle.indexer.RepoFileNameInfo
import com.kodebeagle.logging.Logger

import scala.util.Try
import scala.util.parsing.combinator._

object RepoFileNameParser extends RegexParsers with Logger {

  def apply(input: String): Option[RepoFileNameInfo] = {
    val triedResult1: Try[RepoFileNameParser.ParseResult[RepoFileNameInfo]] = Try(parseAll(repo, input))
    triedResult1.toOption.flatMap {
      case Success(result, _) => Some(result)
      case failure: NoSuccess => log.error(failure.msg)
        None
    }
  }

  def repo: Parser[RepoFileNameInfo] = {
    "(|.*/)repo".r ~> rep(tilde ~> name) ^^ {
      x => val y = x.toArray
        val branch = if(y.size < 7) "master" else y(5)
        val stars = if(y.size  == 8) y(6) else y.last
        val tag = if(y.size == 8) y(7).stripPrefix(".zip").trim else ""
        RepoFileNameInfo(y(0), y(2).toInt, y(1), false, y(4), branch,
          stars.stripSuffix(".zip").trim.toInt, tag)
    }
  }

  def name: Parser[String] = """[^~]+""".r

  def tilde: Parser[String] = """~""".r
}
