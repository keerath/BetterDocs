package com.kodebeagle.actor

import java.net.URI

import akka.actor.{Actor, Props}
import com.kodebeagle.logging.Logger
import org.apache.commons.httpclient.HttpClient
import org.apache.commons.httpclient.methods.{PostMethod, StringRequestEntity}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.collection.mutable.ListBuffer

class ZipTagMaster(batch: String) extends Actor with Logger {
  var nrOfResults: Int = _
  var nrOfWorkers: Int = _
  val start = System.currentTimeMillis()
  val listOfAllResults = ListBuffer[String]()

  override def receive: Receive = {
    case TotalWork(listOfRepoWork) =>
      nrOfWorkers = listOfRepoWork.size
      mkBatchDir
      listOfRepoWork.zipWithIndex.foreach { workWithIndex =>
        val zipTagWorker = context.actorOf(Props(new ZipTagWorker(batch)),
          name = s"ZipTagWorker${workWithIndex._2}")
        zipTagWorker ! workWithIndex._1
      }

    case Result =>
      nrOfResults += 1
      if (nrOfResults == nrOfWorkers) {
        log.info(s"""Time taken ${System.currentTimeMillis() - start}""".stripMargin)
        launchSparkJob()
        context.system.shutdown()
      }
  }

  private def mkBatchDir = {
    val conf = new Configuration()
    conf.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
    conf.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)
    val hdfs = FileSystem.get(URI.create("hdfs://192.168.2.145:9000"), conf)
    hdfs.mkdirs(new Path(s"/user/zips$batch"))
  }

  private def launchSparkJob() = {
    val url = "http://192.168.2.145:6066/v1/submissions/create"
    val client = new HttpClient()
    val postMethod = new PostMethod(url)
    postMethod.addRequestHeader("Content-Type", "application/json")
    val requestJson =
      s"""{
    "action" : "CreateSubmissionRequest",
    "appArgs" : [ $batch ],
    "appResource" : "file:/home/hadoop-user/KodeBeagle-assembly-0.1.4-33-g7611acb.jar",
    "clientSparkVersion" : "1.5.0",
      "environmentVariables" : {
    "SPARK_ENV_LOADED" : "1"
  },
  "mainClass" : "com.kodebeagle.spark.ConsolidatedIndexJob",
  "sparkProperties" : {
    "spark.jars" : "file:/home/hadoop-user/KodeBeagle-assembly-0.1.4-33-g7611acb.jar",
    "spark.driver.supervise" : "false",
    "spark.app.name" : "ConsolidatedIndexJob",
    "spark.driver.memory" : "12g",
    "spark.executor.memory" : "12g",
    "spark.storage.memoryFraction" : "0.1",
    "spark.eventLog.enabled": "false",
    "spark.submit.deployMode" : "cluster",
    "spark.master" : "spark://192.168.2.145:6066"
    }
  }"""
    val entity = new StringRequestEntity(requestJson, "application/json", "UTF-8")
    postMethod.setRequestEntity(entity)
    client.executeMethod(postMethod)
    log.info(postMethod.getResponseBodyAsString)
  }
}
