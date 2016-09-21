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
import com.kodebeagle.indexer.{Docs, TypeAggregator, TypesInFile}
import com.kodebeagle.logging.Logger
import com.kodebeagle.util.SparkIndexJobHelper._
import org.apache.spark.SparkConf

import scala.util.Try

object TypeAggregationJob extends Logger {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setMaster(KodeBeagleConfig.sparkMaster)
      .setAppName("TypeAggregation")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = createSparkContext(conf)
    sc.hadoopConfiguration.set("dfs.replication", "1")

    val typesInfoLocation = Try {
      args(0).trim
    }.toOption.getOrElse(KodeBeagleConfig.typesInfoLocation)

    val javaDocLocation = Try {
      args(1).trim
    }.toOption.getOrElse(KodeBeagleConfig.javaDocLocation)

    // read types in file with no header to ignore
    val typesInFile = parseIndex[TypesInFile](sc.textFile(typesInfoLocation), None)

    val combinedTypesInFile = typesInFile.flatMap(f => {
      val declTypes = f.declaredTypes
        .mapValues((Set.empty[String], _, f.repoName, f.fileName)).toSeq
      val usedTypes = f.usedTypes
        .mapValues(v => (v._1, v._2, f.repoName, f.fileName)).toSeq
      declTypes ++ usedTypes
    })

    val typeAggregates = combinedTypesInFile.aggregateByKey(new TypeAggregator())(
      (agg, value) => agg.merge(value._1, value._2, value._3, value._4),
      (agg1, agg2) => agg1.merge(agg2))

    // read typeDocs
    val javaDocIndex = parseIndex[Docs](sc.textFile(javaDocLocation), None)
    // groupBy type
    val typeVsDocs = javaDocIndex.flatMap(docs => docs.typeDocs.groupBy(_.typeName))
    // and get only propDocs for searchText
    val typeVsPropertyDocs = typeVsDocs.mapValues(_.flatMap(_.propertyDocs))
    // perform a join on the typeName
    val typeAggWithDocs = typeAggregates.join(typeVsPropertyDocs)

    typeAggWithDocs.map { case (typeName, (agg, propDocs)) =>
      toIndexTypeJson("java", "aggregation", agg.result(typeName, propDocs))
    }.saveAsTextFile(s"${KodeBeagleConfig.repoIndicesHdfsPath}Java/types_aggregate")
  }
}
