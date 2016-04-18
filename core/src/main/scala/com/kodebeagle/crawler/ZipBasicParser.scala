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

import java.io._
import java.util.zip.{ZipEntry, ZipInputStream}

import com.kodebeagle.indexer.{RepoFileNameInfo, Repository, Statistics}
import com.kodebeagle.logging.Logger
import com.kodebeagle.spark.Input
import org.apache.commons.compress.archivers.zip.{ZipArchiveEntry, ZipArchiveOutputStream}

import scala.collection.mutable

/**
  * Extracts java files and packages from the given zip file.
  */

object ZipBasicParser extends Logger {

  private val bufferSize = 1024000 // about 1 mb

  private def replaceSpecialChars(tag: String) = tag.replaceAll("kbslash12", "/")

  private def toRepository(mayBeFileInfo: Option[RepoFileNameInfo], stats: Statistics) =
    mayBeFileInfo.map(fileInfo => Repository(fileInfo.login, fileInfo.id, fileInfo.name,
      fileInfo.fork, fileInfo.language, fileInfo.defaultBranch, fileInfo.stargazersCount,
      stats.sloc, stats.fileCount, stats.size, replaceSpecialChars(fileInfo.tag)))

  def readFilesAndPackages(repoFileNameInfo: Option[RepoFileNameInfo],
                           zipStream: ZipInputStream) = {
    val javaFileList = mutable.ArrayBuffer[(String, String)]()
    val scalaFileList = mutable.ArrayBuffer[(String, String)]()
    var size: Long = 0
    var fileCount: Int = 0
    var sloc: Int = 0
    var ze: Option[ZipEntry] = None
    try {
      do {
        ze = Option(zipStream.getNextEntry)
        ze.foreach { ze =>
          if (!ze.isDirectory) {
            val fileName = ze.getName
            log.info(s"Reading file $fileName")
            val fileContent = readContent(zipStream)
            size += fileContent.length
            fileCount += 1
            sloc += fileContent.split("\n").size
            if (ze.getName.endsWith(".scala")) {
              scalaFileList += (fileName -> fileContent)
            } else if (ze.getName.endsWith(".java")) {
              javaFileList += (fileName -> fileContent)
            }
          }
        }
        zipStream.closeEntry()
      } while (ze.isDefined)
    } catch {
      case ex: Exception => log.error("Exception reading next entry {}", ex)
    }

    val fileContentList = (javaFileList.map(_._2) ++ scalaFileList.map(_._2)).toList
    // This is not precise, and might be effected by char-encoding.
    val sizeInKB: Long = size / 1024
    val statistics = Statistics(sloc, fileCount, sizeInKB)
    (javaFileList.toList, scalaFileList.toList, extractPackages(fileContentList),
      toRepository(repoFileNameInfo, statistics))
  }

  private def extractPackages(fileContentList: List[String]) = {
    val allPackages = mutable.Set[String]()
    import scala.io.Source
    fileContentList.foreach { fileContent =>
      val lines = Source.fromString(fileContent).getLines()
      val PACKAGE = "package"
      lines.filter(_.trim.startsWith(PACKAGE)).foreach { line =>
        val strippedLine = line.stripPrefix(PACKAGE).trim
        val indexOfSemiColon = strippedLine.indexOf(";")
        if (indexOfSemiColon == -1) {
          allPackages += strippedLine
        } else {
          allPackages += strippedLine.substring(0, indexOfSemiColon).trim
        }
      }
    }
    allPackages.toList
  }

  def readContent(stream: ZipInputStream): String = {
    val output = new ByteArrayOutputStream()
    var data: Int = 0
    do {
      data = stream.read()
      if (data != -1) output.write(data)
    } while (data != -1)
    val kmlBytes = output.toByteArray
    output.close()
    new String(kmlBytes, "utf-8")
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

  def listAllFiles(dir: String): Array[File] = new File(dir).listFiles
}

object ZipUtil extends Logger {

  def createZip(directoryPath: String, zipPath: String) {
    try {
      val fOut = new FileOutputStream(new File(zipPath))
      val bOut = new BufferedOutputStream(fOut)
      val tOut = new ZipArchiveOutputStream(bOut)
      addFileToZip(tOut, directoryPath, "")
      tOut.finish()
      tOut.close()
      bOut.close()
      fOut.close()
    } catch {
      case ex: Exception => log.error(s"exception while zipping directory $directoryPath", ex)
    }
  }

  def addFileToZip(zOut: ZipArchiveOutputStream, path: String, base: String) {
    val f = new File(path)
    val entryName = base + f.getName
    val zipEntry = new ZipArchiveEntry(f, entryName)
    import org.apache.commons.io.IOUtils
    zOut.putArchiveEntry(zipEntry)
    if (f.isFile) {
      try {
        for {
          fInputStream <- Option(new FileInputStream(f))
        } yield {
          IOUtils.copy(fInputStream, zOut)
          zOut.closeArchiveEntry()
          IOUtils.closeQuietly(fInputStream)
        }
      } catch {
        case ex: Exception => log.error(s"exception while zipping file ${f.getAbsolutePath}", ex)
      }
    } else {
      zOut.closeArchiveEntry()
      val children = f.listFiles()
      if (Option(children).isDefined) {
        for (child <- children) {
          addFileToZip(zOut, child.getAbsolutePath, entryName + "/")
        }
      }
    }
  }
}
