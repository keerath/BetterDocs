/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kodebeagle.spark

import com.kodebeagle.configuration.KodeBeagleConfig
import com.kodebeagle.indexer.{Docs, PropertyDocs}
import com.kodebeagle.util.SparkIndexJobHelper._
import org.apache.spark.SparkConf

case class TypeDoc(typeName: String, typeDoc: String, propDocs: Set[PropertyDocs],
                   score: Long, fileName: String)

object UniqueDocsJob {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster(KodeBeagleConfig.sparkMaster)
      .setAppName("UniqueDocsJob")
    val sc = createSparkContext(conf)
    sc.hadoopConfiguration.set("dfs.replication", "1")

    val docsRDD = parseIndex[Docs](sc.textFile(KodeBeagleConfig.javaDocLocation), None)

    val typeNameVsDocs = docsRDD.flatMap { docs =>
      // 2-tuple with key as type name and value as other entries
      docs.typeDocs.map(typeDocs => (typeDocs.typeName,
        List((typeDocs.typeDoc, typeDocs.propertyDocs, docs.score, docs.fileName))))
    }

    val typeVsUniqDocs = typeNameVsDocs.reduceByKey((accumDocs, docs) =>
      if (accumDocs.size == 1) {
        if (accumDocs.head._3 < docs.head._3) { // comparing scores
          docs
        } else {
          accumDocs
        }
      } else {
        docs
      })

    typeVsUniqDocs.mapValues(_.head).map { case (typeName, (typeDoc,
    propDocs, score, fileName)) => TypeDoc(typeName, typeDoc, propDocs, score, fileName)
    }.saveAsTextFile("/kodebeagle/indices/Java/javadocs")
  }
}
