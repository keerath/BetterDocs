package com.kodebeagle.actor

import akka.actor.{Actor, ActorSystem, Props}
import com.typesafe.config.ConfigFactory
import org.apache.spark.launcher.SparkLauncher


object RemoteActorLauncher extends App {
  val rawConfig  = """akka {
                     |  loglevel = "INFO"
                     |  actor {
                     |    provider = "akka.remote.RemoteActorRefProvider"
                     |  }
                     |  remote {
                     |    enabled-transports = ["akka.remote.netty.tcp"]
                     |    netty.tcp {
                     |      hostname = "192.168.2.77"
                     |      port = 9080
                     |    }
                     |    log-sent-messages = on
                     |    log-received-messages = on
                     |  }
                     |}""".stripMargin

  val config = ConfigFactory.parseString(rawConfig)
  val remoteSystem = ActorSystem("RemoteSystem", config)
  val remote = remoteSystem.actorOf(Props(new RemoteSparkLauncher(args(0), args(1).toInt)), name = "RemoteSparkLauncher")
}

class RemoteSparkLauncher(assemblyPath: String, nrOfResults: Int) extends Actor {
  var resultCounter = 0

  override def receive: Receive = {
    case Result =>
      resultCounter += 1
      if (resultCounter == nrOfResults) {
        launchSparkJob
        resultCounter = 0
      }
    case _ =>
  }

  private def launchSparkJob = {
    val spark = new SparkLauncher().setSparkHome(System.getenv("SPARK_HOME"))
      .setAppName("ConsolidatedIndexJob")
      .setConf("spark.driver.host", "192.168.2.145")
      .setAppResource(assemblyPath).setDeployMode("cluster")
      .setMainClass("com.kodebeagle.spark.ConsolidatedIndexJob").setMaster("spark://HadoopMaster:6066")
      .launch()
    scala.io.Source.fromInputStream(spark.getInputStream).getLines().foreach(println)
    scala.io.Source.fromInputStream(spark.getErrorStream).getLines().foreach(println)
    spark.waitFor()
  }
}
