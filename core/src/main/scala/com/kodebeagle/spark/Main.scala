package com.kodebeagle.spark

import java.io.File

import com.kodebeagle.configuration.KodeBeagleConfig
import com.typesafe.config.{ConfigFactory, Config}
import org.apache.spark.{SparkConf, SparkContext}
import spark.jobserver.{SparkJobInvalid, SparkJobValid, SparkJobValidation, SparkJob}

import scala.util.Try

object Main extends SparkJob {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster(KodeBeagleConfig.sparkMaster)
      .setAppName("ConsolidatedIndexJob")
      .set("spark.driver.memory", "6g")
      .set("spark.executor.memory", "4g")
      .set("spark.network.timeout", "1200s")
    val sc = new SparkContext(conf)
    val config = ConfigFactory.parseString("")
    runJob(sc, config)
  }

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    Try(config.getString("input.serfile"))
      .map(x => SparkJobValid)
      .getOrElse(SparkJobInvalid("No input.serfile config param"))
  }

  override def runJob(sc: SparkContext, config: Config): Unit = {
    config.getString("input.serfile")
    ConsolidatedIndexJob.run(sc)
  }
}
