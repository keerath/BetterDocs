package com.kodebeagle.actor

import akka.actor.{Props, Actor}
import akka.routing.RoundRobinPool
import com.kodebeagle.logging.Logger
import scala.collection.mutable.ListBuffer
import sys.process._

class ZipTagMaster(nrOfWorkers: Int, nrOfReposPerWorker: Int) extends Actor with Logger {
  var nrOfResults: Int = _
  val start = System.currentTimeMillis()
  val tagCheckoutWorker = context.actorOf(RoundRobinPool(nrOfWorkers).props(Props[ZipTagWorker]), "tagCheckoutWorker")
  val listOfAllResults = ListBuffer[String]()
  val remoteActor = context.actorSelection("akka.tcp://RemoteSystem@192.168.2.77:2552/user/RemoteSparkLauncher")

  override def receive: Receive = {
    case Work(listOfRepoWork) =>
      listOfRepoWork.grouped(nrOfReposPerWorker).foreach(tagCheckoutWorker ! Work(_))

    case Result =>
      nrOfResults += 1
      if (nrOfResults == nrOfWorkers) {
        log.info(s"""Time taken ${System.currentTimeMillis() - start}""".stripMargin)
        remoteActor ! Result

        this.context.system.shutdown()
      }
  }

  private def doCleanUp() = {

  }
}
