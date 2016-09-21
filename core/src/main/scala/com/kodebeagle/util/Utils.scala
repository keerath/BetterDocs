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

package com.kodebeagle.util

import java.io.{BufferedWriter, ByteArrayOutputStream, OutputStreamWriter}
import java.net.URI
import java.util.zip.{ZipEntry, ZipInputStream}

import com.kodebeagle.indexer.{RepoFileNameInfo, Repository, Statistics}
import com.kodebeagle.javaparser.MethodInvocationResolver.MethodDecl
import com.kodebeagle.logging.Logger
import com.kodebeagle.spark.TypeAggregationJob._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization._

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.Try

object Utils extends Logger {

  def write(outputPath: String, record: (Int, Int, String), conf: Configuration): Unit = {
    val outFile = s"$outputPath/${record._1}-${record._2}"

    val fs = FileSystem.get(URI.create(outFile), conf)

    val writer = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(outFile), true)))
    writer.write(record._3)
    writer.close()
  }

  private def toRepository(mayBeFileInfo: Option[RepoFileNameInfo], stats: Statistics) =
    mayBeFileInfo.map(fileInfo => Repository(fileInfo.login, fileInfo.id, fileInfo.name,
      fileInfo.fork, fileInfo.language, fileInfo.defaultBranch, fileInfo.stargazersCount,
      stats.sloc, stats.fileCount, stats.size))

  def readContent(stream: ZipInputStream): String = {
    val output = new ByteArrayOutputStream()
    var data: Int = 0
    do {
      data = stream.read()
      if (data != -1) output.write(data)
    } while (data != -1)
    val kmlBytes = output.toByteArray
    output.close()
    new String(kmlBytes, "utf-8").trim.replaceAll("\t", "  ")

  }

  def generateMethodSignature(methodDecl: MethodDecl): String = {
    var returnType = methodDecl.getReturnType
    if (returnType == "void") {
      returnType = ""
    }

    val name = methodDecl.getMethodName

    val args = methodDecl.getArgs.map { case (varName, varType) =>
      if (varName.length <= 3) {
        ("", varType)
      } else {
        (varName, varType)
      }
    }.map { case (varName, varType) => s"$varName $varType" }
      .mkString(" ")

    s"$returnType $name $args"
  }

  def readJSFiles(repoFileNameInfo: Option[RepoFileNameInfo],
                  zipStream: ZipInputStream): (List[(String, String)], Option[Repository]) = {
    val list = mutable.ArrayBuffer[(String, String)]()
    var size = 0
    var sloc = 0
    var fileCount = 0
    var ze: Option[ZipEntry] = None
    try {
      do {
        ze = Option(zipStream.getNextEntry)
        ze.foreach { ze => if (ze.getName.endsWith(".js") && !ze.isDirectory
          && !ze.getName.contains("node_modules")) {
          val fileName = ze.getName
          val fileContent = readContent(zipStream)
          size += fileContent.length
          fileCount += 1
          sloc += fileContent.split("\n").size
          list += (fileName -> fileContent)
        }
        }
        zipStream.closeEntry()
      } while (ze.isDefined)
    } catch {
      case ex: Exception => log.error("Exception reading next entry {}", ex)
    } finally {
      zipStream.close()
    }
    val sizeInKB = size / 1024
    val stats = Statistics(sloc, fileCount, size)
    (list.toList, toRepository(repoFileNameInfo, stats))
  }
}


