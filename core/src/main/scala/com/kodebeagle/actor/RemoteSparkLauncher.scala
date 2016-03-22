package com.kodebeagle.actor

import akka.actor.{Actor, ActorSystem, Props}
import com.typesafe.config.ConfigFactory
import org.apache.spark.launcher.SparkLauncher


object RemoteActorLauncher extends App {
  val config = ConfigFactory.load("remoteSparkLauncher.conf")
  val remoteSystem = ActorSystem("RemoteSystem", config)
  val remote = remoteSystem.actorOf(Props(new RemoteSparkLauncher(args(0))), name = "RemoteSparkLauncher")
}

class RemoteSparkLauncher(assemblyPath: String) extends Actor {
  var resultCounter = 0

  override def receive: Receive = {
    case Result =>
      if(resultCounter == 4) {
        launchSparkJob
      } else {
        resultCounter+=1
      }
    case _ =>
  }

  private def launchSparkJob = {
    val spark = new SparkLauncher().setSparkHome(System.getenv("SPARK_HOME"))
      .setAppName("ConsolidatedIndexJob")
      .setAppResource(assemblyPath).setDeployMode("cluster")
      .setMainClass("com.kodebeagle.spark.ConsolidatedIndexJob").setMaster("spark://HadoopMaster:6066")
      .launch()
    scala.io.Source.fromInputStream(spark.getInputStream).getLines().foreach(println)
    scala.io.Source.fromInputStream(spark.getErrorStream).getLines().foreach(println)
    spark.waitFor()
  }
}
