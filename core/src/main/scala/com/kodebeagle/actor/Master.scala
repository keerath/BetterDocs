package com.kodebeagle.actor

import akka.actor.{Props, Actor}
import akka.routing.RoundRobinPool
import com.kodebeagle.logging.Logger
import scala.collection.mutable.ListBuffer

class Master(nrOfWorkers: Int, nrOfReposPerWorker: Int) extends Actor with Logger {
  var nrOfResults: Int = _
  val start = System.currentTimeMillis()
  val tagCheckoutWorker = context.actorOf(RoundRobinPool(nrOfWorkers).props(Props[ZipTagWorker]), "tagCheckoutWorker")
  val listOfAllResults = ListBuffer[String]()

  override def receive: Receive = {
    case Work(listOfRepoWork) =>
      listOfRepoWork.grouped(nrOfReposPerWorker).foreach(tagCheckoutWorker ! Work(_))

    case Result =>
      nrOfResults += 1
      if (nrOfResults == nrOfWorkers) {
        log.info(s"""Time taken ${(System.currentTimeMillis() - start) / 1000}""".stripMargin)
        this.context.system.shutdown()
      }
  }
}
