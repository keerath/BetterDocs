package com.kodebeagle.crawler

import com.kodebeagle.configuration.KodeBeagleConfig

import scala.io.Source
import org.json4s._
import org.json4s.jackson.JsonMethods._

sealed trait Message
case class RepoUrlMessage(url: String) extends Message

object Crawler {

  val listOfURLs = Source.fromFile(KodeBeagleConfig.repoURLS)
    .getLines().map(x => RepoUrlMessage(parse(x) \ "repository_url".asInstanceOf[JString])).toList

}


class Master(listOfURls: List[RepoUrlMessage])



