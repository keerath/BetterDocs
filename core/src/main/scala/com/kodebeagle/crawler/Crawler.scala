package com.kodebeagle.crawler

import akka.actor.Actor
import com.kodebeagle.configuration.KodeBeagleConfig
import com.kodebeagle.logging.Logger
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.jsoup.Jsoup

import scala.concurrent.Future
import scala.io.Source

sealed trait Message

case class RepoURLMsg(url: String) extends Message

case object StartCrawl extends Message

object Crawler {

  val listOfURLs = Source.fromFile(KodeBeagleConfig.repoURLS)
    .getLines().map(x => makeRepoURLMsg(parse(x) \ "repository_url")).filter(_.url.nonEmpty).toList


  def makeRepoURLMsg(jValue: JValue) = {
    RepoURLMsg(jValue match {
      case str: JString => str.values
      case _ => ""
    })
  }
}


class Master(listOfURls: List[RepoURLMsg]) extends Actor {

  override def receive: Receive = {

    case StartCrawl => listOfURls.foreach(x =>)

    case _ =>
  }
}


class RepoDownloader extends Actor with Logger {

  import scala.collection.JavaConversions._

  override def receive: Receive = {
    case RepoURLMsg(url) => Future {
      Jsoup.connect(url).get()
    }.onSuccess {
      case doc =>
        val stars = doc.select("a[href$=stargazers]").head.text().trim.toInt // get stars
        val branch = doc.select("span[class=js-select-button css-truncate-target]").head.text().trim // get default branch
        val orgAndRepoName = Utils.extractOrgAndRepoName(url)
        val organization = orgAndRepoName._1
        val repoName = orgAndRepoName._2
        val clone = // to do GithubApiHelper.cloneRepository
    }
  }

}

object Utils extends Logger {

  def extractOrgAndRepoName(url: String) = {
    val arr = url.stripPrefix("http://github.com/").split("/")
    (arr(0), arr(1))
  }
}
