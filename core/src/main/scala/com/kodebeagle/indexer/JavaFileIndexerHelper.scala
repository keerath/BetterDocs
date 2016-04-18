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

object JavaFileIndexerHelper {
  val penalizeTestFiles = 5

  def fileNameToURL(repo: Repository, f: String): String = {
    val (_, actualFileName) = f.splitAt(f.indexOf('/'))
    val branchOrTag = if(repo.tag == "HEAD") {
      repo.defaultBranch
    } else {
      repo.tag
    }
    s"""${repo.login}/${repo.name}/blob/$branchOrTag$actualFileName"""
  }

  def isTestFile(imports: Set[(String, String)]): Boolean = {
    imports.exists(x => x._1.equalsIgnoreCase("org.junit"))
  }

  def tuple2ToImportString(importName: (String, String)): String = {
    importName._1 + "." + importName._2
  }
}

