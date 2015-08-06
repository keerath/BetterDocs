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

import java.io.InputStream

import com.imaginea.kodebeagle.util.EditorDocOps
import com.intellij.openapi.editor.Document
import com.intellij.openapi.editor.impl.DocumentImpl
import org.apache.commons.io.IOUtils
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.collection.JavaConversions._

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
    val imports = Set(
    "import java.io.BufferedInputStream",
    "import java.io.FileInputStream",
    "import java.nio.channels.FileChannel",
    "import java.util.ArrayList",
    "import java.util.HashSet",
    "import java.util.List",
    "import java.util.Set",
    "import java.util.Iterator",
    "import java.io.File",
    "import javax.swing.JComponent"
    )
    val excludeImport = Set(
      "java.util.Array*",
      "java.io.*InputStream",
      "javax.swing.*"
    )
    val editorDocOps = new EditorDocOps().excludeConfiguredImports(imports,excludeImport)
    print(editorDocOps.toSet)
    val expectedImports = Set(
      "import java.nio.channels.FileChannel",
      "import java.util.Set",
      "import java.util.Iterator",
      "import java.util.List",
      "import java.util.HashSet",
      "import java.io.File"
    )
    assert(editorDocOps.toSet === expectedImports)
  }
  ignore("Internal imports should be excluded from imports") {
    test("Internal imports should be excluded from imports") {
      val imports = Set(
        "import java.io.BufferedInputStream",
        "import java.io.FileInputStream",
        "import java.nio.channels.FileChannel",
        "import com.imagenia.pramati.MojoP",
        "import com.imagenia.pramati.Plan"
      )
      val internalImports = Set(
        "import com.imagenia.pramati.MojoP",
        "import com.imagenia.pramati.Plan"
      )
      val editorDocOps = new EditorDocOps().excludeInternalImports(imports)
      val expectedImports = Set(
        "import java.io.BufferedInputStream",
        "import java.io.FileInputStream",
        "import java.nio.channels.FileChannel"
      )
      assert(editorDocOps.toSet === expectedImports)
    }
  }
}
