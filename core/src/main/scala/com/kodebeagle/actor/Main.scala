package com.kodebeagle.actor

import java.io.File


import akka.actor.{Props, ActorSystem}
import com.kodebeagle.indexer.RepoFileNameInfo
import com.kodebeagle.parser.RepoFileNameParser
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.index.query.QueryBuilders

import scala.collection.JavaConversions._

object Main {
  val lookUp = esFetchRepoIdAndTags

  def main(args: Array[String]): Unit = {
    val listOfFiles = listFiles("/home/keerathj/Desktop/repos")
    val listOfRepoNameVsPath = listOfFiles.flatMap(tupleOfRepoNameVsPath)
    val listOfRepoWork = listOfRepoNameVsPath.map(toRepoWork)
    val master = initActorSystem(listOfRepoWork.size)
    master ! Work(listOfRepoWork)
  }

  private def initActorSystem(numOfRepos: Int) = {
    val actorSystem = ActorSystem("TagBasedIndexer")
    val numOfWorkers = Runtime.getRuntime.availableProcessors() * 2
    val numOfReposPerWorker = numOfRepos / numOfWorkers
    actorSystem.actorOf(Props(new Master(numOfWorkers,
      numOfReposPerWorker)), name = "Master")
  }

  private def toRepoWork(tupleOfRepoNameVsPath: (RepoFileNameInfo, String)) = {
    val repoFileNameInfo = tupleOfRepoNameVsPath._1
    val repoPath = tupleOfRepoNameVsPath._2
    val indexedTags = getIndexedTags(repoFileNameInfo)
    RepoWork(repoFileNameInfo, repoPath, indexedTags)
  }

  private def getIndexedTags(repoFileNameInfo: RepoFileNameInfo) = {
    lookUp.find(_._1 == repoFileNameInfo.id).map(_._2).getOrElse(Set[String]())
  }

  private def tupleOfRepoNameVsPath(file: File) = {
    val name = RepoFileNameParser(file.getName)
    val path = file.getAbsolutePath
    if (name.isDefined) {
      Some((name.get, path))
    } else {
      None
    }
  }

  private def listFiles(path: String) = {
    new File(path).listFiles().filter(_.getName.endsWith(".zip")).toList
  }

  private def esFetchRepoIdAndTags = {

    def transformMap(esMap: Map[String, AnyRef]) =
      esMap.get("id").get.toString.toInt -> esMap.get("tag").get.toString

    val clusterName = "kodebeagle1"
    val hostName = "localhost"
    val port = 9300
    val size = 1000
    val client = new TransportClient(ImmutableSettings.settingsBuilder()/*.put("cluster.name", clusterName)*/
      .put("client.transport.sniff", true).build()).addTransportAddress(new InetSocketTransportAddress(hostName, port))

    val searchResponse = client.prepareSearch("repo")
      .setFetchSource(Array("id", "tag"), Array[String]())
      .setQuery(QueryBuilders.boolQuery().must(QueryBuilders.matchAllQuery()))
      .setSize(size).get()

    searchResponse.getHits.getHits.map(_.getSource.toMap)
      .toList.map(transformMap).groupBy(_._1)
      .mapValues(x => x.map(_._2).toSet)
  }
}
