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

package com.kodebeagle.spark

import java.util.ArrayList

import codemining.java.codeutils.JavaASTExtractor
import codemining.languagetools.ParseType
import codesum.lm.main.ASTVisitors.TreeCreatorVisitor
import codesum.lm.main.Settings
import com.kodebeagle.util.SparkIndexJobHelper._
import com.kodebeagle.configuration.{KodeBeagleConfig, TopicModelConfig}
import com.kodebeagle.logging.Logger
import com.kodebeagle.ml.{DistributedLDAModel, LDA}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.eclipse.jdt.core.dom.CompilationUnit
import org.elasticsearch.spark._


import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

object CreateTopicModelJob extends Logger {

  case class Node(name: String, parentId: Array[Long], id: Long = -22) extends Serializable
  case class TopicModel(model: Map[String, Object])

  val jobName = TopicModelConfig.jobName
  val nbgTopics = TopicModelConfig.nbgTopics
  val nIterations = TopicModelConfig.nIterations
  val nWordsDesc = TopicModelConfig.nDescriptionWords
  val chkptInterval = TopicModelConfig.chkptInterval
  val batchSize = TopicModelConfig.batchSize
  val esPortKey = "es.port"
  val esNodesKey = "es.nodes"
  val topicFieldName = "topic"
  val termFieldName = "term"
  val freqFieldName = "freq"
  val fileFieldName = "file"
  val klScoreFieldName = "klscore"
  val filesFieldName = "files"
  val slocSize = 400000
  val repoSizeInSloc = 200000

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster(KodeBeagleConfig.sparkMaster).
      set("spark.kryoserializer.buffer.max","128m").setAppName(jobName)
    conf.set(esNodesKey, KodeBeagleConfig.esNodes)
    conf.set(esPortKey, KodeBeagleConfig.esPort)
    var sc: SparkContext = createSparkContext(conf)
    sc.setCheckpointDir(KodeBeagleConfig.sparkCheckpointDir)
    val esRepoStatistics =fetchReposStatisticsFromEs(sc)

    val repoMap = esRepoStatistics.map{
      case (repoId,(sloc,repo)) => (repoId, repo)
    }.collect()
    val repoStatistics = esRepoStatistics.filter(_._2._1 < repoSizeInSloc).
      sortBy(_._2._1).mapValues(_._1.toLong).collect()
    val batches = getBatches(repoStatistics).sortBy(_.map(_._2).sum).reverse
    var batch = -1
    batches.map(x => {
      sc.stop()
      sc = createSparkContext(conf)
      sc.setCheckpointDir(KodeBeagleConfig.sparkCheckpointDir)
      batch = batch + 1
      log.info("batch "  + batch + " : " + x.map(_._1).toList + " total sloc " + x.map(_._2).sum)
      runOnRepos(sc, x.map{case (x,y) => x.toString}, repoMap.toMap)
    })
  }

  private def getBatches(repoStatistics: Array[(Long,Long)]) = {
    var size = 0L
    val repoStatSize = repoStatistics.size
    var batches = List[Array[(Long, Long)]]()
    var repoStat = Array[(Long, Long)]()
    var i = 0
    var j= 0
    while (i < repoStatSize) {
      val (id, reposize) = repoStatistics(i)
      size += reposize
      if (size <= slocSize && j < batchSize) {
        repoStat = repoStat ++ Array((id, reposize))
        j = j + 1
      } else {
        size = reposize
        batches +:= repoStat
        repoStat = Array[(Long, Long)]()
        repoStat = repoStat ++ Array((id, reposize))
        j= 1
      }
      if(i == repoStatSize-1) {
        batches +:= repoStat
      }
      i = i + 1
    }
    batches
  }
  case class RepositoryModel(repoId: Long, repoName: String, repoStars: Long,
                             login: String, language: String)
  private def fetchReposStatisticsFromEs(sc: SparkContext):
  RDD[(Long, (Long, RepositoryModel))] = {
    val repoStatistics = sc.esRDD("statistics/typestatistics")
      .map({
      case (repoId, valuesMap) => {
        val repositoryId =valuesMap.get("repoId").getOrElse(0).asInstanceOf[Long]
        val repo = new RepositoryModel(repositoryId, valuesMap.get("repoName").
          getOrElse("").asInstanceOf[String], valuesMap.get("repoStars").
          getOrElse(0).asInstanceOf[Int], valuesMap.get("login").
          getOrElse("").asInstanceOf[String],
          valuesMap.get("language").getOrElse("").asInstanceOf[String])
        (repositoryId, (valuesMap.get("sloc").getOrElse(0).asInstanceOf[Long],repo))
      }
    })
    log.info(s"s for repos: repos stats fetched: ${repoStatistics.count()}")
    repoStatistics
  }

  /**
   * Helper functions
   */
  private def fetchReposFromEs(sc: SparkContext,
                               repoIds: Array[String]): RDD[(Int, (String, String))] = {
    var ids = repoIds.mkString(",")
    val query = s"""{"query":{"terms": {"repoId": [${ids}]}}}"""

    val repoSources = sc.esRDD(KodeBeagleConfig.esourceFileIndex, Map("es.query" -> query))
      .map({
      case (repoId, valuesMap) => {
        (valuesMap.get("repoId").getOrElse(0).asInstanceOf[Int],
          (valuesMap.get("fileName").getOrElse("").asInstanceOf[String],
            valuesMap.get("fileContent").getOrElse("").toString))
      }
    })
    log.info(s"Querying files for repos: $query , files fetched: ${repoSources.count()}")
    repoSources
  }

  private def runOnRepos(sc: SparkContext, repoIds: Array[String],
                         repoMap: Map[Long, RepositoryModel]) = {
    val offset = nbgTopics
    val repoSources = fetchReposFromEs(sc, repoIds)
    val repoIdVsFiles = repoSources.groupBy(_._1)
    val repos = repoIdVsFiles.map(f => new Node(f._1.toString(), Array(0)))
    val (repoNameVsId, repoIdVsName, repoVertices) = extractVocab(repos, offset)
    val fileIdOffset = repoNameVsId.size + offset
    val files = repoSources.map({
      case (repoId, (fileName, fileContent)) =>
        new Node(repoId + ":" + fileName, Array(repoNameVsId.get(repoId.toString()).get))
    })
    val (fileNameVsId, fileIdVsName, fileVertices) = extractVocab(files, fileIdOffset)
    val paragraphIdOffset = fileNameVsId.size + repoNameVsId.size + offset
    val paragraph = getParagraphTokens(repoSources, repoNameVsId, fileNameVsId)

    val paragraphVertices = paragraph.map(f => (f,"")).keys.zipWithIndex().map({
      case (paragraphNode,paragraphId) =>
        (paragraphId + paragraphIdOffset, paragraphNode)
    }).persist()
    val words = paragraphVertices.flatMap{
      case (k,v) =>
        val tokens =  v.name.split(" ").distinct.map { t =>
          new Node(t, Array(k))
        }
        tokens
    }.persist()
    val wordIdOffset = fileNameVsId.size + repoNameVsId.size + paragraphVertices.count() + offset
    val wordVocab = words.map(f => (f.name, "")).aggregateByKey(0)((x, y) => 0, (l, r) => 0)
      .keys.zipWithIndex().map({
      case (word, wordId) =>
        (word, wordId + wordIdOffset)
    }).collectAsMap()
    val tokenToWordMap = wordVocab.map({ case (word, wordId) => (wordId, word) }).toMap
    val edges = words.map(f => (wordVocab.get(f.name).get, f.parentId(0))).cache()
    val paragraphGroupings = paragraphVertices.flatMap({
      case (paragraphId, paragraphNode) =>
        List((paragraphId, paragraphNode.parentId(0))) ++ List((paragraphId,
          paragraphNode.parentId(1)))
    }).persist()
    val result = new LDA().setMaxIterations(nIterations)
      .setK(nbgTopics).setCheckPointInterval(chkptInterval)
      .runFromEdges(edges, Option(paragraphGroupings))
    handleResult(result, sc, repoIdVsName, tokenToWordMap,fileIdVsName, repoMap)

    paragraphVertices.unpersist()
    words.unpersist()
    edges.unpersist()
  }

  def getParagraphTokens(repoSources: RDD[(Int, (String, String))],
                         repoNameVsId: mutable.Map[String, Long],
                         fileNameVsId: mutable.Map[String, Long]): RDD[Node] = {
    repoSources.map({
      case (repoId, (fileName, fileContent)) =>
        (fileName, (repoId, fileContent))
    }).flatMap({
      case (fileName, (repoId, fileContent)) =>
        val paragraphListTry = Try(getParagraphForFile(fileContent))
        var paragraphList: java.util.List[String] = new java.util.ArrayList[String]()
        if (paragraphListTry.isSuccess) {
          paragraphList = paragraphListTry.get
        }
        if (paragraphList.isEmpty) {
          paragraphList.add("this is to ignore files which are not clean")
        }
        val paragraphTokens = paragraphList.map { t =>
          new Node(t, Array(fileNameVsId.get(repoId + ":" + fileName).get,
            repoNameVsId.get(repoId.toString).get))
        }
        paragraphTokens
    })
  }

  def handleResult(result: DistributedLDAModel,
                   sc: SparkContext,
                   repoIdVsName: mutable.Map[Long, String],
                   tokenToWordMap: Map[Long, String],
                   fileIdVsName: mutable.Map[Long, String],
                   repoMap: Map[Long, RepositoryModel]): Unit = {
    val topics = result.describeTopics(nWordsDesc)
    logTopics(topics, repoIdVsName, tokenToWordMap)
    var i = nbgTopics
    val repoTopics = topics.slice(nbgTopics, topics.length)
    val repoTopicFields = for {topic <- repoTopics if repoIdVsName.get(i.toLong).isDefined } yield
    { // For each repo, map of topic terms vs their frequencies
      val repoName = repoIdVsName.get(i.toLong).get
      val topicMap = topic.map({
        case (count, wordId) =>
          (tokenToWordMap(wordId.toLong), count)
      }).toMap
      i += 1
      (repoName, topicMap.toSeq.sortWith(_._2 > _._2))
    }
    val repoSummary = result.summarizeDocGroups()
    logRepoSummary(repoSummary, repoIdVsName, fileIdVsName)
    val repoSummaryRdd = sc.makeRDD(repoSummary)
    val repoFilescore = repoSummaryRdd.map{ // For each repo, map of files vs their score.
      case(repoId, fileId, klScore) =>
        repoIdVsName.get(repoId).map(repoName => (repoName,
          fileIdVsName.get(fileId *(-1L)).get.split(":")(1), klScore))
    }.filter(_.isDefined).map(_.get).groupBy(f => f._1)
      .map(f => (f._1, f._2.map({case (repoId, file, klscore) =>
      (file, klscore)}).toSeq.sortBy(_._2)))

    val updatedRepoRDD = sc.makeRDD(repoTopicFields).join(repoFilescore)
      .map({ case(repoId, (repoTopicMap, repoFileMap)) =>
        val totalFrequency = repoTopicMap.toMap.values.sum
        val esTopicMap = repoTopicMap.map { f =>
          Try(f._2 / totalFrequency.toDouble)  match {
            case Success(d) => Map(termFieldName -> f._1,
              freqFieldName -> d)
            case Failure(ex) => Map()
          }
        }.filter(_.nonEmpty)
      val esFilesMap = repoFileMap.map(f => Map(fileFieldName-> f._1, klScoreFieldName-> f._2))
      val termMap = Map(topicFieldName -> esTopicMap)
      val topicMap = Map(filesFieldName -> esFilesMap)
      Map("_id" -> repoId) ++ Map("repository" -> repoMap.get(repoId.toLong)) ++
        termMap ++ topicMap
    })
    saveTopicModelTokens(updatedRepoRDD)
  }

  def saveTopicModelTokens(updatedRepoRDD: RDD[Map[String, Object]]): Unit = {
    if (TopicModelConfig.save) {
      updatedRepoRDD.map(repoRecord => toJson(TopicModel(repoRecord))).
        saveAsTextFile(TopicModelConfig.saveToLocal + java.util.UUID.randomUUID())
    } else {
      updatedRepoRDD.saveToEs(KodeBeagleConfig.esRepoTopicIndex,
        Map("es.write.operation" -> "upsert",
          "es.mapping.id" -> "_id"))
    }
  }

  def logTopics(topics: Array[Array[(Int, Long)]],
                repoIdVsName: mutable.Map[Long, String],
                tokenToWordMap: Map[Long, String]): Unit = {
    val minFreq = 0
    for (i <- 0 until topics.size-1) {
      var topicName = "Background Topic "
      if (i < nbgTopics) {
        topicName = topicName + i
      } else {
        topicName = repoIdVsName.getOrElse(i.toLong, "")
      }
      if (topicName != "") {
        log.info(s"Topic Description for topic : $topicName")
        val sortedCounts = topics(i).sortBy(f => f._1).reverse
        sortedCounts.filter(x => x._1 > minFreq).foreach(z =>
          log.info(s"${z._1} : ${tokenToWordMap(z._2.toLong)}"))
      }
    }
  }

  def logRepoSummary(repoSummary: Array[(Long, Long, Double)],
                     repoIdVsName: mutable.Map[Long, String],
                     fileIdVsName: mutable.Map[Long, String]): Unit = {
    repoSummary.groupBy(_._1).foreach(f => {
      if (repoIdVsName.get(f._1).getOrElse("") != "") {
        log.info(s"Top docs for repo : ${repoIdVsName.get(f._1).getOrElse("Anonymous")}")
        f._2.sortBy(_._3).filter(f =>
          !(fileIdVsName.get((f._2 * (-1L))).getOrElse("").contains("test"))).take(20)
          .foreach(f =>
          log.info(s"${f._3} - ${fileIdVsName.get((f._2 * (-1L))).get.split(":")(1)}"))
      }
    })
  }

  private def isNumeric(s: String) = s forall Character.isDigit

  def getParagraphForFile(fileContent: String): java.util.List[String] = {
    val tcv = new TreeCreatorVisitor()
    val ext = new JavaASTExtractor(false, true)
    val node = ext.getAST(fileContent, ParseType.COMPILATION_UNIT).asInstanceOf[CompilationUnit]
    tcv.process(node, fileContent, new Settings)

    val tokenList = new ArrayList[String]()
    val nodeCount = tcv.getTree().getNodeCount()
    val list = List("the","get","string","name","set","for","this","java",
      "license","under","file","equals","add", "exception")
    var nodeID = 0
    // Save foldable node tokens ordered by nodeID
    for (nodeID <- 0 until nodeCount) {
      val sb = new StringBuilder()
      for (s <- tcv.getIDTokens().get(nodeID)) {
        if (!s.isEmpty && !list.contains(s) && !isNumeric(s) && s.length > 2) {
          sb.append(s + " ")
        }
      }
      tokenList.add(sb.toString() + " ")
    }
    tokenList
  }

  def extractVocab(tokens: RDD[Node], startIndex: Int):
  (mutable.Map[String, Long], mutable.Map[Long, String], RDD[(Long, Node)]) = {
    val vocab = tokens.map(x => x.name).distinct().collect()
    var vocabLookup = mutable.Map[String, Long]()
    for (i <- 0 to (vocab.length - 1)) {
      vocabLookup += (vocab(i) -> (startIndex + i))
    }
    val nodesWithIds = tokens.map { x =>
      val id = vocabLookup.get(x.name).get
      val node = new Node(x.name, x.parentId, id)
      (id, node)
    }
    (vocabLookup, vocabLookup.map({ case (name, id) => (id, name) }), nodesWithIds)
  }
}
