package com.kodebeagle.spark

import com.kodebeagle.hadoop.ZipFileInputFormat
import com.kodebeagle.indexer.Repository
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by keerathj on 15/4/16.
  */
object TestJob extends App {

  val conf = new SparkConf()
  conf.setMaster("local[2]").setAppName("hello")
  val sc = new SparkContext(conf)
  val rdd = sc.newAPIHadoopFile[Option[Repository], Input, ZipFileInputFormat]("hdfs://192.168.2.145:9000/user/199/*").cache()
  val count = rdd.count()
  println(count)
  println(rdd.take(4).map(_._1.get.id).toList)
}