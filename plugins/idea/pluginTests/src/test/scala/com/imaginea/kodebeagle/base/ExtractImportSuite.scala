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

import java.io.InputStream

import com.imaginea.kodebeagle.impl.util.JavaImportsUtil
import com.intellij.openapi.editor.Document
import com.intellij.openapi.editor.impl.DocumentImpl
import org.apache.commons.io.IOUtils
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable

class ExtractImportSuite extends FunSuite with BeforeAndAfterAll {
  private val stream: InputStream =
    Thread.currentThread.getContextClassLoader.getResourceAsStream("TestData.java")

  private val fileContents = IOUtils.readLines(stream).mkString("\n")

  private val document: Document = new DocumentImpl(fileContents)

  /*
   * Currently we are not getting Project object while executing tests.
   * We need to explore this thing to get the Project object
   * TODO : Explore mocking
   */

  test("Excluded imports set should accept regex and FQCN.") {
    val importVsMethods = new mutable.HashMap[String, java.util.Set[String]]()
    importVsMethods +=(
      "java.io.BufferedReader" -> Set("read").asJava,
      "java.io.FileInputStream" -> Set("close").asJava,
      "java.nio.channels.FileChannel" -> Set("lock").asJava,
      "java.util.ArrayList" -> Set("addAll", "add").asJava,
      "java.util.HashSet" -> Set("put").asJava,
      "java.util.List" -> Set("add").asJava,
      "java.util.Set" -> Set("retainAll").asJava,
      "java.util.Iterator" -> Set("hasNext", "next").asJava,
      "java.io.File" -> Set("isDirectory").asJava,
      "javax.swing.JComponent" -> Set("disable").asJava)
    val excludeImport = Set(
      "java.util.Array*",
      "java.io.*InputStream",
      "javax.swing.*",
      "java.util.Iterator"
    )
    val editorDocOps =
      new JavaImportsUtil().excludeConfiguredImports(importVsMethods.asJava, excludeImport.asJava)
    val expectedImports = Set(
      "java.util.List",
      "java.nio.channels.FileChannel",
      "java.util.HashSet",
      "java.util.Set",
      "java.io.BufferedReader",
      "java.io.File"
    )
    assert(editorDocOps.keySet().toSet === expectedImports.toSet)
  }
  ignore("Internal imports should be excluded from imports") {
    test("Internal imports should be excluded from imports") {
      val imports = Map(
        "java.io.BufferedInputStream" -> Set("read").asJava,
        "java.io.FileInputStream" -> Set("close").asJava,
        "java.nio.channels.FileChannel" -> Set("lock").asJava,
        "com.imaginea.pramati.MojoP" -> Set("setMojo").asJava,
        "com.imaginea.pramati.Plan" -> Set("setPlan").asJava
      )
      val internalImports = Set(
        "com.imaginea.pramati.MojoP",
        "com.imaginea.pramati.Plan"
      )
      val editorDocOps = new JavaImportsUtil().excludeInternalImports(imports.asJava)
      val expectedImports = Set(
        "java.io.BufferedInputStream",
        "java.io.FileInputStream",
        "java.nio.channels.FileChannel"
      )
      assert(editorDocOps.keySet === expectedImports)
    }
  }
}
