package com.kodebeagle.actor

import java.io.File


import akka.actor.{Props, ActorSystem}
import com.kodebeagle.indexer.RepoFileNameInfo
import com.kodebeagle.parser.RepoFileNameParser
import com.typesafe.config.ConfigFactory
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.index.query.QueryBuilders

import scala.collection.JavaConversions._

object ZipTagLauncher {
  val lookUp = esFetchRepoIdAndTags

  def main(args: Array[String]): Unit = {
    val listOfFiles = listFiles(args(0))
    val listOfRepoNameVsPath = listOfFiles.flatMap(tupleOfRepoNameVsPath)
    val listOfRepoWork = listOfRepoNameVsPath.map(toRepoWork)
    val master = initActorSystem(listOfRepoWork.size)
    master ! Work(listOfRepoWork)
  }

  private def initActorSystem(numOfRepos: Int) = {
    val rawConfig = """akka {
                      |  loglevel = "INFO"
                      |  actor {
                      |    provider = "akka.remote.RemoteActorRefProvider"
                      |  }
                      |  remote {
                      |    enabled-transports = ["akka.remote.netty.tcp"]
                      |    netty.tcp {
                      |      hostname = "127.0.0.1"
                      |      port = 2552
                      |    }
                      |    log-sent-messages = on
                      |    log-received-messages = on
                      |  }
                      |}""".stripMargin

    val config = ConfigFactory.parseString(rawConfig)
    val actorSystem = ActorSystem("ZipForEachTag", config)
    val numOfWorkers = numOfRepos
    val numOfReposPerWorker = 1
    actorSystem.actorOf(Props(new ZipTagMaster(numOfWorkers,
      numOfReposPerWorker)), name = "ZipTagMaster")
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
    val hostName = "172.16.12.21"
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
