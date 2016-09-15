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

import com.kodebeagle.configuration.KodeBeagleConfig
import com.kodebeagle.indexer.{ContextProperty, Line, PayloadProperty, PropertyDocs, RepoFileNameInfo, Repository, SourceFile, Statistics}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.JsonAST.{JArray, JField, JInt, JObject, JString, JValue}
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write
import org.json4s.{CustomSerializer, NoTypeHints, _}

import scala.io.Source
import scala.util.Try

object SparkIndexJobHelper {

  def fileNameToURL(repo: Repository, f: String): String = {
    val (_, actualFileName) = f.splitAt(f.indexOf('/'))
    s"""${repo.login}/${repo.name}/blob/${repo.defaultBranch}$actualFileName"""
  }

  def mapToSourceFile(repo: Option[Repository],
                      file: (String, String)): SourceFile = {
    val repo2 = repo.getOrElse(Repository.invalid)
    SourceFile(repo2.id, fileNameToURL(repo2, file._1), file._2)
  }

  def createSparkContext(conf: SparkConf): SparkContext = {
    val sc = new SparkContext(conf)
    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")
    sc
  }

  private def extractRepoDirName(x: String) = x.substring(0, x.indexOf('/'))


  private def getStats(fContent: String) = {
    val lines = Source.fromString(fContent).getLines()
    val sloc = lines.size
    val count = 1
    val pkg = List(extractPackage(lines))
    val size = fContent.length
    (sloc, count, pkg, size)
  }

  def extractPackage(lines: Iterator[String]): String = {
    val PACKAGE = "package "
    var pkg = ""
    lines.find(_.trim.startsWith(PACKAGE)).foreach { line =>
      val strippedLine = line.stripPrefix(PACKAGE).trim
      val indexOfSemiColon = strippedLine.indexOf(";")
      if (indexOfSemiColon == -1) {
        pkg = strippedLine
      } else {
        pkg = strippedLine.substring(0, indexOfSemiColon).trim
      }
    }
    pkg
  }

  private def toRepository(mayBeFileInfo: Option[RepoFileNameInfo], stats: Statistics) =
    mayBeFileInfo.map(fileInfo => Repository(fileInfo.login, fileInfo.id, fileInfo.name,
      fileInfo.fork, fileInfo.language, fileInfo.defaultBranch, fileInfo.stargazersCount,
      stats.sloc, stats.fileCount, stats.size))

  private def toStatistics(sloc: Int, count: Int, size: Int) = Statistics(sloc, count, size)

  def makeRDD(sc: SparkContext, batch: String): RDD[(String, (String, String))] = {
    val inputDir = s"${KodeBeagleConfig.githubDir}/$batch/"
    val rdd = sc.wholeTextFiles(s"$inputDir*")
      .map { case (fName, fContent) =>
        val cleanedFName = fName.stripPrefix("file:").stripPrefix("hdfs:").stripPrefix(inputDir)
        (cleanedFName, fContent)
      }
      .map { case (fName, fContent) => (extractRepoDirName(fName), (fName, fContent)) }
      .persist(StorageLevel.MEMORY_AND_DISK)
    rdd
  }


  def createRepoIndex(rdd: RDD[(String, (String, String))],
                      batch: String): Map[String, (Option[Repository], List[String])] = {
    val aggregateRDD = rdd
      .map { case (repoDirName, (_, fContent)) => (repoDirName, getStats(fContent)) }
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2, x._3 ++ y._3, x._4 + y._4))
      .map { case (repoDirName, (tSloc, tCount, tPkgs, tSize)) =>
        (repoDirName, (toRepository(RepoFileNameParser(repoDirName),
          toStatistics(tSloc, tCount, tSize)), tPkgs))
      }.cache()
    aggregateRDD.map(_._2._1.getOrElse(Repository.invalid)).filter(x => x != Repository.invalid)
      .flatMap(repo => Seq(toJson(repo)))
      .saveAsTextFile(KodeBeagleConfig.sparkIndexOutput + batch + "repoIndex")
    aggregateRDD.collectAsMap().toMap
  }

  /**
    * This currently uses star counts for a repo as a score.
    */
  def getGitScore(f: String): Option[Int] = {
    Try(f.stripSuffix(".zip").split("~").last.toInt).toOption
  }

  def getOrgsName(f: String): Option[String] = {
    Try(f.stripSuffix(".zip").split("~").tail.head).toOption
  }

  def toJson[T <: AnyRef <% Product with Serializable](t: Set[T]): String = {
    (for (item <- t) yield toJson(item)).mkString("\n")
  }

  def toIndexTypeJson[T <: AnyRef <% Product with Serializable](indexName: String,
                                                                typeName: String, t: Set[T]
                                                               ): String = {
    (for (item <- t) yield toIndexTypeJson(indexName, typeName, item)).mkString("\n")
  }

  case class IndexHeader(index: IndexValue)

  case class IndexValue(_index: String, _type: String, _id: String)

  def toIndexTypeJson[T <: AnyRef <% Product with Serializable](indexName: String,
                                                                typeName: String, t: T,
                                                                idopt: Option[String] = None
                                                               ): String = {

    implicit val formats = Serialization.formats(NoTypeHints) + new ContextPropertySerializer +
      new PayloadPropertySerializer + new PropertyDocsSerializer

    val header = idopt match {
      case None => s"""{ "index" : {"_index" : "$indexName", "_type" : "$typeName" }"""
      case Some(id) => write(IndexHeader(IndexValue(indexName, typeName, id)))
    }
    header + "\n" + write(t)
  }

  def toJson[T <: AnyRef <% Product with Serializable](t: T): String = {

    implicit val formats = Serialization.formats(NoTypeHints) + new ContextPropertySerializer +
      new PayloadPropertySerializer + new PropertyDocsSerializer

    val indexName = t.productPrefix.toLowerCase
    "" + write(t)
  }

  class PayloadPropertySerializer extends CustomSerializer[PayloadProperty](format => ({

    case JObject(List(JField("name", JString(propNameWithArgs)),
    JField("lines", JArray(lines)))) =>
      val propNameAndArgs = getPropNameAndArgs(propNameWithArgs)
      PayloadProperty(propNameAndArgs._1, propNameAndArgs._2, deSerLines(lines))
  }, {
    case PayloadProperty(name, args, lines) =>
      val nameWithArgs = toNameWithArgs(name, args)
      JObject(List(JField("name", JString(nameWithArgs)), JField("lines", serLines(lines))))
  }))

  private def serLines(lines: Set[Line]) = JArray(lines.map(line => JArray(List(JInt(line.line),
    JInt(line.startCol), JInt(line.endCol)))).toList)

  private def deSerLines(lines: List[JValue]) =
    lines.map { jValue =>
      val linesArr = jValue.asInstanceOf[JArray].arr.toArray
      val lineNum = linesArr(0).asInstanceOf[JInt].num.toInt
      val startCol = linesArr(1).asInstanceOf[JInt].num.toInt
      val endCol = linesArr(2).asInstanceOf[JInt].num.toInt
      Line(lineNum, startCol, endCol)
    }.toSet

  class ContextPropertySerializer extends CustomSerializer[ContextProperty](format => ({
    case JString(prop: String) => val (name, args) = getPropNameAndArgs(prop)
      ContextProperty(name, args)
  }, {
    case ctxProp: ContextProperty => JString(toNameWithArgs(ctxProp.name, ctxProp.args))
  }))


  class PropertyDocsSerializer extends CustomSerializer[PropertyDocs](format => ({
    case JObject(List(JField("propertyName", JString(nameAndArgs)),
    JField("propertyDoc", JString(propDoc)))) =>
      val (name, args) = getPropNameAndArgs(nameAndArgs)
      PropertyDocs(name, args, propDoc)
  }, {
    case propDocs: PropertyDocs => JObject(List(JField("propertyName",
      JString(toNameWithArgs(propDocs.propertyName, propDocs.argTypes))),
      JField("propertyDoc", JString(propDocs.propertyDoc))))
  }))

  private def toNameWithArgs(name: String, args: List[String]): String =
    s"$name(${args.mkString(",")})"

  private def getPropNameAndArgs(prop: String) = {
    val indexOfParen = prop.indexOf('(')
    val propName = prop.substring(0, indexOfParen)
    val propArgs = prop.substring(indexOfParen + 1).stripSuffix(")").split(",").toList
    (propName, propArgs)
  }
}
