package com.kodebeagle.actor

import java.io.File

import akka.actor.{ActorSystem, Props}
import com.kodebeagle.indexer.RepoFileNameInfo
import com.kodebeagle.logging.Logger
import com.kodebeagle.parser.RepoFileNameParser
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.index.query.QueryBuilders

import scala.collection.JavaConversions._

object ZipTagLauncher extends Logger {
  val lookUp = esFetchRepoIdAndTags
  var listOfFolders: List[String] = _


  def main(args: Array[String]) = {
    val listOfFolders = listFolders(args(0))
    listOfFolders.foreach { folder =>
        val actorSystem = initActorSystem()
        start(folder._1, actorSystem, folder._2)
        actorSystem.awaitTermination()
      log.info(s">> Batch $folder completed !")
    }
  }

  def listFolders(batchDirectory: String) = {
    new File(batchDirectory).listFiles().filter(_.isDirectory).map(x=> (x.getAbsolutePath, x.getName.toInt)).sortBy(_._2).toList
  }

  def start(batchFolder: String, actorSystem: ActorSystem, batch: Int): Unit = {
    val listOfFiles = listFiles(batchFolder)
    val listOfRepoNameVsPath = listOfFiles.flatMap(tupleOfRepoNameVsPath)
    val listOfRepoWork = listOfRepoNameVsPath.map(toRepoWork)
    val master = initZipTagMaster(listOfRepoWork.size, batch.toString, actorSystem)
    master ! TotalWork(listOfRepoWork)
    log.info(s"Number of repos ${listOfRepoWork.size}")
  }

  private def initActorSystem() = ActorSystem("ZipForEachTag")

  private def initZipTagMaster(numOfRepos: Int, batch: String, actorSystem: ActorSystem) =
    actorSystem.actorOf(Props(new ZipTagMaster(batch)), name = "ZipTagMaster")

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
    val hostName = "192.168.2.77"
    val port = 9301
    val size = 1000
    val client = new TransportClient(ImmutableSettings.settingsBuilder() /*.put("cluster.name", clusterName)*/
      .put("client.transport.sniff", true).build()).addTransportAddress(new InetSocketTransportAddress(hostName, port))

    val searchResponse = client.prepareSearch("repo")
      .setFetchSource(Array("id", "tag"), Array[String]())
      .setQuery(QueryBuilders.boolQuery().must(QueryBuilders.matchAllQuery()))
      .setSize(size).get()

    val lookUp = searchResponse.getHits.getHits.map(_.getSource.toMap)
      .toList.map(transformMap).groupBy(_._1)
      .mapValues(x => x.map(_._2).toSet)
    client.close()
    lookUp
  }
}
