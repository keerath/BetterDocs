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

package com.kodebeagle.ml

import org.apache.spark.graphx.{Edge, EdgeTriplet, Graph, PartitionID, PartitionStrategy, TripletFields, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.{Logging, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.PriorityQueue
import scala.util.Random

/**
 *
 * Latent Dirichlet Allocation (LDA), a topic model designed for text documents.
 *
 * Terminology:
 *  - "word" = "term": an element of the vocabulary
 *  - "token": instance of a term appearing in a document
 *  - "topic": multinomial distribution over words representing some concept
 *
 * Currently, the underlying implementation uses Expectation-Maximization (EM), implemented
 * according to the Asuncion et al. (2009) paper referenced below.
 *
 * References:
 *  - Original LDA paper (journal version):
 *    Blei, Ng, and Jordan.  "Latent Dirichlet Allocation."  JMLR, 2003.
 *     - This class implements their "smoothed" LDA model.
 *  - Paper which clearly explains several algorithms, including EM:
 *    Asuncion, Welling, Smyth, and Teh.
 *    "On Smoothing and Inference for Topic Models."  UAI, 2009.
 *
 * Note: This implementation uses the basic implementation of LDA as here by github user
 * EntilZha and builds on top of it.
 * See LDA.scala in https://github.com/EntilZha/spark/blob/LDA-Refactor/mllib/
 */

class LDA private (
                    private var k: Int,
                    private var maxIterations: Int,
                    private var checkPointInterval: Int,
                    private var topicSmoothing: Array[Double],
                    private var termSmoothing: Array[Double],
                    private var seed: Long,
                    private var algorithm: LDA.LearningAlgorithm.LearningAlgorithm,
                    private var logStats: Boolean,
                    private var paramOptimizeInterval: Int) extends Logging {

  import LDA._

  def this() = this(k = 3, maxIterations = 20, checkPointInterval = 100,
    topicSmoothing = Array(), termSmoothing = Array(), seed = Random.nextLong(),
    algorithm = LDA.LearningAlgorithm.Gibbs, logStats = false, paramOptimizeInterval = 100)

  /**
   * Number of background topics to infer.  I.e., the number of soft cluster centers.
   * (default = 3)
   */
  def getK: Int = k

  def setK(k: Int): this.type = {
    require(k > 0, s"LDA k (number of clusters) must be > 0, but was set to $k")
    this.k = k
    this
  }

  def getLogStats: Boolean = logStats

  def setLogStats(logStats: Boolean): this.type = {
    this.logStats = logStats
    this
  }

  def getParamOptimizeInterval: Int = paramOptimizeInterval

  def setParamOptimizeInterval(paramOptimizeInterval: Int): this.type = {
    this.paramOptimizeInterval = paramOptimizeInterval
    this
  }
  /**
   * Topic smoothing parameter (commonly named "alpha").
   *
   * This is the parameter to the Dirichlet prior placed on the per-document topic distributions
   * ("theta").  We use a symmetric Dirichlet prior.
   *
   * This value should be > 0.0, where larger values mean more smoothing (more regularization).
   * If set to -1, then topicSmoothing is set automatically.
   *  (default = -1 = automatic)
   *
   * Automatic setting of parameter:
   *  - For EM: default = (50 / k) + 1.
   *     - The 50/k is common in LDA libraries.
   *     - The +1 follows Asuncion et al. (2009), who recommend a +1 adjustment for EM.
   */
  def getTopicSmoothing: Array[Double] = topicSmoothing

  def getTopicSmoothing(nt: Int): Array[Double] = {
    val topicSmoothing = if (this.topicSmoothing.size == nt) {
      this.topicSmoothing
    } else {
      // (50.0 / k) + 1.0
      val alpham = new Array[Double](nt)
      var i = 0
      while (i < k) {
        // alpham(i) = (50.0 / k) + 1.0
        alpham(i) = 1.7
        i = i + 1
      }
      while (i < nt) {
        // alpham(i) = (50.0 / nVisibleTopics) + 1.0
        alpham(i) = 2.3
        i = i + 1
      }
      alpham
    }
    topicSmoothing
  }

  def setTopicSmoothing(topicSmoothing: Array[Double]): this.type = {
    require(topicSmoothing.size == 0 || topicSmoothing.filter(x => x > 0.0).size > 0,
      s"LDA topicSmoothing weights must be > 0 (or empty for auto), but was set to $topicSmoothing")
    // TODO: Check each value too
    this.topicSmoothing = topicSmoothing
    this
  }

  /**
   * Term smoothing parameter (commonly named "eta").
   *
   * This is the parameter to the Dirichlet prior placed on the per-topic word distributions
   * (which are called "beta" in the original LDA paper by Blei et al., but are called "phi" in many
   *  later papers such as Asuncion et al., 2009.)
   *
   * This value should be > 0.0.
   * If set to -1, then termSmoothing is set automatically.
   *  (default = -1 = automatic)
   *
   * Automatic setting of parameter:
   *  - For EM: default = 0.1 + 1.
   *     - The 0.1 gives a small amount of smoothing.
   *     - The +1 follows Asuncion et al. (2009), who recommend a +1 adjustment for EM.
   */
  def getTermSmoothing: Array[Double] = termSmoothing

  def getTermSmoothing(nt: Int): Array[Double] = {
    val termSmoothing = if (this.termSmoothing.size == nt) {
      this.termSmoothing
    } else {
      // 1.1
      val betam = new Array[Double](nt)
      var i = 0
      while (i < k) {
        betam(i) = 1.1
        i = i + 1
      }
      while (i < nt) {
        betam(i) = 0.1
        i = i + 1
      }
      betam
    }
    termSmoothing
  }

  def setTermSmoothing(termSmoothing: Array[Double]): this.type = {
    require(termSmoothing.size == 0 || termSmoothing.filter(x => x > 0.0).size > 0,
      s"LDA termSmoothing must be > 0 (or empty for auto), but was set to $termSmoothing")
    // TODO: check each element
    this.termSmoothing = termSmoothing
    this
  }

  /**
   * Maximum number of iterations for learning.
   * (default = 20)
   */
  def getMaxIterations: Int = maxIterations

  def setMaxIterations(maxIterations: Int): this.type = {
    this.maxIterations = maxIterations
    this
  }

  /**
   * Checkpoint interval.
   * (default = every 100 iterations)
   */
  def getCheckPointInterval: Int = checkPointInterval

  def setCheckPointInterval(checkPointInterval: Int): this.type = {
    require(checkPointInterval > 0, "Checkpoint interval should be a positive value")
    this.checkPointInterval = checkPointInterval
    this
  }

  /** Random seed */
  def getSeed: Long = seed

  def setSeed(seed: Long): this.type = {
    this.seed = seed
    this
  }

  def runFromEdges(edges: RDD[(WordId, DocId)],
                   documentGroupings: Option[RDD[(DocId, GroupId)]] = None): DistributedLDAModel = {
    // No. of document specific topics: should be same for all documents 
    var distinctDocPvtTopics = 0
    var perDocPvtTopics = 0
    var groupedDocsOpt: Option[RDD[(DocId, Array[GroupId])]] = None
    // Find out the groups to which each document is assigned. 
    // Their number should be same for all documents
    if (documentGroupings.isDefined) {
      val groupedDocs = documentGroupings.get.map(f => (-(f._1 + 1), f._2)).groupBy(_._1).map({
        case (docId, itr) =>
          (docId, itr.map(_._2).toArray)
      })
      val max = groupedDocs.map(_._2.length).max
      val min = groupedDocs.map(_._2.length).min
      assert(max == min,
        s"Each document is not assigned to an equal number of groups. Max: $max, and Min: $min")
      distinctDocPvtTopics = groupedDocs.map(_._2).
        flatMap { x => x.toList }.distinct().count().toInt
      perDocPvtTopics = max
      groupedDocsOpt = Option(groupedDocs)
    }
    // Topics visible to words in a doc : public topics + doc specific topics.
    val nVisibleTopics = k + perDocPvtTopics
    // Initialize parameters
    val topicSmoothing: Array[Double] = getTopicSmoothing(nVisibleTopics)
    val termSmoothing: Array[Double] = getTermSmoothing(nVisibleTopics)
    assert(topicSmoothing.length == termSmoothing.length
      && termSmoothing.length == (k + perDocPvtTopics),
      "Improper setup of parameters")
    var state = getState(edges, distinctDocPvtTopics, perDocPvtTopics,
      groupedDocsOpt, topicSmoothing, termSmoothing)
    var iter = 0
    while (iter < maxIterations) {
      if (iter % checkPointInterval == 0 && iter > 0) checkpoint(state)
      if (paramOptimizeInterval > 0 && iter % paramOptimizeInterval ==0){
        state.optimizeParams()
      }
      state = state.next()
      iter += 1
    }
    if(logStats) {
      state.logPerformanceStatistics()
    }
    new DistributedLDAModel(state)
  }

  def getState(edges: RDD[(WordId, DocId)], distinctDocPvtTopics: PartitionID,
               perDocPvtTopics: PartitionID, groupedDocsOpt: Option[RDD[(DocId, Array[GroupId])]],
               topicSmoothing: Array[Double],
               termSmoothing: Array[Double]): GibbsLearningState = {
    algorithm match {
      case LearningAlgorithm.Gibbs => LDA.GibbsLearningState.initialStateFromEdges(
        edges, groupedDocsOpt, k,
        distinctDocPvtTopics, perDocPvtTopics, topicSmoothing,
        termSmoothing, seed)
    }
  }

  def checkpoint(state: LDA.GibbsLearningState): Unit = {

    /**
     * Need to do this because - if an RDD is materialized before
     * marking it for checkpoint the checkpointing doesnt work.
     */
    state.graph = Graph(state.graph.vertices, state.graph.edges)
      .partitionBy(PartitionStrategy.EdgePartition1D)
    val g = state.graph
    val e = g.edges
    val v = g.vertices
    g.checkpoint()
    v.count()
    e.count()
    assert(state.graph.isCheckpointed, "Graph not checkpointed")
  }
}

/**
 * LDA contains utility methods used in the LDA class. These are mostly
 * methods which will be serialized during computation so cannot be methods.
 */
object LDA {
  object LearningAlgorithm extends Enumeration {
    type LearningAlgorithm = Value
    val Gibbs, EM = Value
  }
  type TopicCounts = Array[Int]
  type DocId = Long
  type WordId = Long
  type GroupId = Long
  def isDocumentVertex(v: Long): Boolean = v < 0
  def isWordVertex(v: Long): Boolean = v >= 0
  trait LearningState {
    def logPrior(): Double
    def logLikelihood(): Double
    def collectTopicTotals(): TopicCounts
    def describeTopics(maxTermsPerTopic: Long): Array[Array[(Int, Long)]]
    def summarizeDocGroups(groupIndex: Int): Array[(GroupId, DocId, Double)]
    def next(): LearningState
    def optimizeParams(): Unit
    def vocabSize(): Long
    def k: Int
  }

  trait LearningStateInitialization {
    def initialStateFromEdges(edges: RDD[(WordId, DocId)],
                              groupedDocs: Option[RDD[(Long, Array[GroupId])]],
                              k: Int,
                              distinctDocPvtTopics: Int,
                              perDocPvtTopics: Int,
                              alpha: Array[Double],
                              beta: Array[Double],
                              seed: Long): LearningState
  }

  object GibbsLearningState extends Serializable with LearningStateInitialization {
    /**
     * These two variables are used for turning indexing on and off when creating new histograms
     */
    val IndexTrue: DocId = -1L
    val IndexFalse: WordId = 1L
    /**
     * Topic Long is composed of two Ints representint topics.
     * The left Int represents the current topic, right Int represents prior topic.
     */
    type Topic = Long

    case class Histogram(counts: TopicCounts, docTopics: Array[Long]) extends Serializable

    case class Posterior(docs: VertexRDD[Histogram], words: VertexRDD[Histogram])

    def getCurrentTopic(topic: Topic): Int = {
      (topic >> 32).asInstanceOf[Int]
    }

    def getOldTopic(topic: Topic): Int = {
      topic.asInstanceOf[Int]
    }

    def combineTopics(currentTopic: Int, oldTopic: Int): Topic = {
      (currentTopic.asInstanceOf[Long] << 32) | (oldTopic & 0xffffffffL)
    }

    /**
     * Sums two factors together into a, then returns it. This increases memory efficiency
     * and reduces garbage collection.
     * @param a First factor
     * @param b Second factor
     * @return Sum of factors
     */
    def combineCounts(a: TopicCounts, b: TopicCounts): TopicCounts = {
      assert(a.size == b.size)
      var i = 0
      while (i < a.size) {
        a(i) += b(i)
        i += 1
      }
      a
    }

    /**
     * Combines a topic into a factor
     * @param a Factor to add to
     * @param topic topic to add
     * @return Result of adding topic into factor.
     */
    def combineTopicIntoCounts(a: TopicCounts, topic: Topic): TopicCounts = {
      a(getCurrentTopic(topic)) += 1
      a
    }

    def combineDeltaIntoCounts(a: TopicCounts, topic: Topic): TopicCounts = {
      a(getOldTopic(topic)) -= 1
      a(getCurrentTopic(topic)) += 1
      a
    }

    /**
     * Creates a new histogram then applies the deltas in place to maintain the
     * argsort order. It is assumed that deltas are sparse so that
     * the bubblesort used fast.
     * @param oldHistogram
     * @param deltas
     * @return
     */
    def applyDeltasToHistogram(oldHistogram: Histogram,
                               deltas: Array[Int],
                               vid: Long): Histogram = {
      val counts = new TopicCounts(oldHistogram.counts.length)
      var i = 0
      while (i < counts.length) {
        counts(i) = oldHistogram.counts(i) + deltas(i)
        assert(counts(i) >= 0,
          "\n Vid : " + vid +
            "\n Old histogram: " + oldHistogram.counts.mkString(",") +
            "\n Delta :" + deltas.mkString(" , "))
        i += 1
      }
      makeHistogramFromCounts(counts, oldHistogram.docTopics)
    }

    /**
     * Creates a factor with topic added to it.
     * @param nTopics Number of topics
     * @param topic Topic to start with
599     * @return New factor with topic added to it
     */
    def makeCountsFromTopic(nTopics: Int, topic: Topic): TopicCounts = {
      val counts = new TopicCounts(nTopics)
      counts(getCurrentTopic(topic)) += 1
      counts
    }

    /**
     * Creates a new histogram from counts, counts becomes part of the new histogram
     * object.
     * @param counts
     * @param vid
     * @param parameter Parameter for computing norm sum, either alpha or beta
     * @return
     */
    def makeHistogramFromCounts(counts: TopicCounts, docSpecificTopics: Array[Long]): Histogram = {
      Histogram(counts, docSpecificTopics)
    }

    def makeEmptyHistogram(nTopics: Int, nDocSpecificTopics: Int): Histogram = {
      Histogram(new TopicCounts(nTopics), new Array[Long](nDocSpecificTopics))
    }

    /**
     * Extracts the vocabulary from the RDD of tokens. Returns a Map from each word to its unique
     * number key, and an array indexable by that number key to the word
     * @param tokens RDD of tokens to create vocabulary from
     * @return array and map for looking up words from keys and keys from words.
     */
    def extractVocab(tokens: RDD[String]): (Array[String], mutable.Map[String, WordId]) = {
      val vocab = tokens.distinct().collect()
      var vocabLookup = mutable.Map[String, WordId]()
      for (i <- 0 to vocab.length - 1) {
        vocabLookup += (vocab(i) -> i)
      }
      (vocab, vocabLookup)
    }

    /**
     * Extracts edges from an RDD of documents. Each document is a single line/string
     * in the RDD
     * @param lines RDD of documents
     * @param vocab Vocabulary in the documents
     * @param vocabLookup Vocabulary lookup in the documents
     * @param delimiter Delimiter to split on
     * @return RDD of edges between words and documents representing tokens.
     */
    def edgesFromTextDocLines(lines: RDD[String],
                              vocab: Array[String],
                              vocabLookup: mutable.Map[String, WordId],
                              delimiter: String = " "): RDD[(WordId, DocId)] = {
      val sc = lines.sparkContext
      val numDocs = lines.count()
      val docsWithIds = lines.zipWithUniqueId()
      val edges: RDD[(WordId, DocId)] = docsWithIds.flatMap {
        case (line: String, docId: DocId) =>
          val words = line.split(delimiter)
          val docEdges = words.map(word => (vocabLookup(word), docId))
          docEdges
      }
      edges
    }

    /**
     * Re-samples the given token/triplet to a new topic
     * @param randomDouble Random number generator
     * @param totalHistogram roadcast Total histogram of topics
     * @param nt Number of topics
     * @param alpha Parameter for dirichlet prior on per document topic distributions
     * @param beta Parameter for the dirichlet prior on per topic word distributions
     * @param nw Number of words in corpus
     * @return New topic for token/triplet
     */
    def sampleToken(randomDouble: Double,
                    topic: Topic,
                    docHistogram: Histogram,
                    wordHistogram: Histogram,
                    totalHistogram: Histogram,
                    nt: Int,
                    pvtTopics: Array[Long],
                    alpha: Array[Double],
                    beta: Array[Double],
                    nw: Long): Topic = {
      val totalCounts = totalHistogram.counts
      val wHist = wordHistogram.counts
      val dHist = docHistogram.counts
      val oldTopic = getCurrentTopic(topic)
      val nVisibleTopics = nt + pvtTopics.length
      assert(wHist(oldTopic) > 0,
        "Illegal state: word assigned to topic for which histogram count is 0.")
      assert(dHist(oldTopic) > 0)
      assert(dHist(oldTopic) > 0,
        "Illegal state: word in doc assigned to topic for which histogram count is 0.")
      assert(totalCounts(oldTopic) > 0)
      assert(nVisibleTopics == alpha.length && nVisibleTopics == beta.length)
      // Construct the conditional
      val conditional = new Array[Double](nVisibleTopics)
      var t = 0
      var conditionalSum = 0.0
      while (t < conditional.size) {
        val index = if (t < nt) t else pvtTopics(t - nt).toInt
        val cavityOffset = if (index == oldTopic) 1 else 0
        val w = wHist(index) - cavityOffset
        val d = dHist(index) - cavityOffset
        val total = totalCounts(index) - cavityOffset
        conditional(t) = (alpha(t) + d) * (beta(t) + w) / (beta(t) * nw + total)
        conditionalSum += conditional(t)
        t += 1
      }
      assert(conditionalSum > 0.0)
      // Generate a random number between [0, conditionalSum)
      val u = randomDouble * conditionalSum
      assert(u < conditionalSum)
      // Draw the new topic from the multinomial
      var newTopic = 0
      var cumsum = conditional(newTopic)
      while (cumsum < u) {
        newTopic += 1
        cumsum += conditional(newTopic)
      }
      newTopic = if (newTopic < nt) newTopic else pvtTopics(newTopic - nt).toInt
      combineTopics(newTopic, oldTopic)
    }

    def countWithoutTopic(counts: TopicCounts, index: Int, topic: Int): Int = {
      if (index == topic) {
        counts(index) - 1
      } else {
        counts(index)
      }
    }

    def vertices(edges: RDD[Edge[Topic]],
                 getVertex: Edge[Topic] => VertexId,
                 nTopics: Int,
                 nDocTopics: Int,
                 vertexType: VertexId): RDD[(Long, Histogram)] = {
      edges.map { e => (getVertex(e), e) }.aggregateByKey(makeEmptyHistogram(nTopics, nDocTopics))({
        case (histogram, edge) =>
          val t = getCurrentTopic(edge.attr)
          histogram.counts(t) += 1
          histogram
      }, {
        case (l, r) =>
          Histogram(combineCounts(l.counts, r.counts), l.docTopics)
      })
    }

    def initialStateFromEdges(edges: RDD[(WordId, DocId)],
                              groupedDocs: Option[RDD[(Long, Array[GroupId])]],
                              k: Int,
                              distinctDocPvtTopics: Int,
                              perDocPvtTopics: Int,
                              alpha: Array[Double],
                              beta: Array[Double],
                              seed: Long): GibbsLearningState = {
      val state = new GibbsLearningState(edges, groupedDocs, k,
        distinctDocPvtTopics, perDocPvtTopics, alpha, beta, 100, false, false)
      state.setup()
      state
    }
  }

  /**
   * LDA contains the model for topic modeling using Latent Dirichlet Allocation
   * @param tokens RDD of edges, transient to insure it doesn't get sent to workers
   * @param nTopics Number of topics
   * @param alpha Model parameter, governs sparsity in document-topic mixture
   * @param beta Model parameter, governs sparsity in word-topic mixture
   * @param loggingInterval Interval for logging
   * @param loggingLikelihood If true, log the likelihood
   * @param loggingTime if true, log the runtime of each component
   */
  private[ml] class GibbsLearningState(@transient val tokens: RDD[(WordId, DocId)],
                                       @transient val docGroups:
                                       Option[RDD[(Long, Array[GroupId])]],
                                       val nTopics: Int = 100,
                                       val nDocTopics: Int = 0,
                                       val nPrivateTopics: Int = 0,
                                       val alpha: Array[Double],
                                       val beta: Array[Double],
                                       val loggingInterval: Int = 0,
                                       val loggingLikelihood: Boolean = false,
                                       val loggingTime: Boolean = false) extends LearningState
  with Serializable with Logging {
    import GibbsLearningState._
    // var timer: TimeTracker = null
    private var sc: Option[SparkContext] = None
    // Create an empty graph to keep scalastyle happy for null.
    var graph: Graph[Histogram, Topic] = Graph(tokens.sparkContext.emptyRDD[(Long, Histogram)],
      tokens.sparkContext.emptyRDD[Edge[Topic]])
    var nWords: Long = 0
    var nDocs: Long = 0
    var nTokens: Long = 0
    var totalHistogram: Option[Histogram] = None
    var resampleTimes: Option[mutable.ListBuffer[Long]] = None
    var updateCountsTimes: Option[mutable.ListBuffer[Long]] = None
    var globalCountsTimes: Option[mutable.ListBuffer[Long]] = None
    var likelihoods: Option[mutable.ListBuffer[Double]] = None
    private var iteration = 0
    var modelIsSetup: Boolean = false

    def describeTopics(maxTermsPerTopic: Long): Array[Array[(Int, Long)]] = {
      topWords(maxTermsPerTopic.toInt)
    }

    def collectTopicTotals(): TopicCounts = throw new NotImplementedError()

    def logPrior(): Double = throw new NotImplementedError()

    def k: Int = nTopics

    def vocabSize(): Long = {
      nWords
    }

    /**
     * Get the word vertices by filtering on non-negative vertices
 *
     * @return Word vertices
     */
    def wordVertices: VertexRDD[Histogram] = graph.vertices.filter { case (vid, c) => vid >= 0 }

    /**
     * Get the document vertices by filtering on negative vertices
 *
     * @return Document vertices
     */
    def docVertices: VertexRDD[Histogram] = graph.vertices.filter { case (vid, c) => vid < 0 }

    def setup(): Unit = {
      // timer = new TimeTracker()
      resampleTimes = Option(new mutable.ListBuffer[Long]())
      updateCountsTimes = Option(new mutable.ListBuffer[Long]())
      globalCountsTimes = Option(new mutable.ListBuffer[Long]())
      likelihoods = Option(new mutable.ListBuffer[Double]())
      // timer.start("setup")
      logInfo("Starting LDA setup")
      sc = Option(tokens.sparkContext)
      /**
       * The bipartite terms by document graph.
       */
      graph = createInitialGraph().cache()

      nWords = wordVertices.count()
      nDocs = docVertices.count()
      nTokens = graph.edges.count()
      /**
       * The total counts for each topic
       */
      val counts = graph.edges.map(e => e.attr)
        .aggregate(new TopicCounts(nTopics + nDocTopics))(combineTopicIntoCounts, combineCounts)
      totalHistogram = Option(makeHistogramFromCounts(
        counts,
        Array()))
      logInfo("LDA setup finished")
      // timer.stop("setup")
      modelIsSetup = true

    }

    def createInitialGraph(): Graph[Histogram, Topic] = {
      val nT = nTopics
      val nd = nDocTopics
      val nPt = nPrivateTopics
      // Make the docids positive here.
      val dg = docGroups.get.map(f => (-f._1-1, f._2))
      val edges: RDD[Edge[Topic]] = tokens.map({
        case(wid, did)=> (did, wid) // just reverse it
      }).leftOuterJoin(dg).mapPartitionsWithIndex({
        // join the each edge tuple with its doc groups  
        case (pid, iterator) =>
          iterator.map({
            case (docId, (wordId, groups)) =>
              assert(wordId >= 0)
              assert(docId >= 0)
              val newDocId: DocId = -(docId + 1L)
              val gen = new java.util.Random(wordId)
              var selectedIndex = gen.nextInt(nT + nPt)
              // If index more than bgtopics, then chose that topic from groupids

              val docTopicIndex = selectedIndex - nT
              if (docTopicIndex >= 0) {
                selectedIndex = groups.get(docTopicIndex).toInt
              }
              Edge(wordId, newDocId, combineTopics(selectedIndex, 0))
          })
      })
      edges.cache

      val setupWordVertices = vertices(edges, _.srcId, nT + nd, nPt, IndexFalse)
      var setupDocVertices = vertices(edges, _.dstId, nT + nd, nPt, IndexTrue)

      // At each doc vertex store what are the doc specific topics.
      if (docGroups.isDefined) {
        val docVerticesWithGroups = setupDocVertices.leftOuterJoin(docGroups.get).map({
          case (docId, (histogram, docArr)) =>
            (docId, Histogram(histogram.counts, docArr.get))
        })
        setupDocVertices = docVerticesWithGroups
      }
      var g = Graph(setupDocVertices ++ setupWordVertices, edges)
        .partitionBy(PartitionStrategy.EdgePartition1D)
      g
    }

    def next(): GibbsLearningState = {
      // Log the negative log likelihood
      if (loggingLikelihood && iteration % loggingInterval == 0) {
        val likelihood = logLikelihood()
        likelihoods.get += likelihood
      }
      // Shadowing because scala's closure capture would otherwise serialize the model object
      val a = alpha
      val b = beta
      val nt = nTopics
      val nd = nDocTopics
      val nw = nWords

      // Broadcast the topic histogram
      val totalHistogramBroadcast = sc.get.broadcast(totalHistogram)

      // Re-sample all the tokens
      var tempTimer: Long = System.nanoTime()
      val parts = graph.edges.partitions.size
      val interIter = iteration
      graph = graph.mapTriplets({
        (pid: PartitionID, iter: Iterator[EdgeTriplet[Histogram, Topic]]) =>
          val initSeed = parts * interIter + pid
          val gen = new java.util.Random(initSeed)
          iter.map({ token =>
            val u = gen.nextDouble()
            val newTopic = sampleToken(u, token.attr, token.dstAttr,
              token.srcAttr, totalHistogramBroadcast.value.get,
              nt, token.dstAttr.docTopics, a, b, nw)
            newTopic
          })
      }, TripletFields.All).cache

      if (loggingTime && iteration % loggingInterval == 0 && iteration > 0) {
        resampleTimes.get += System.nanoTime() - tempTimer
      }

      // Update the counts
      updateGraph();
      this
    }

    def updateGraph(): Unit = {
      var tempTimer = System.nanoTime()
      val deltas = graph.edges
        .flatMap(e => {
        val topic = e.attr
        val old = getOldTopic(topic)
        val current = getCurrentTopic(topic)
        var result: Iterator[(VertexId, Topic)] = Iterator.empty
        if (old != current) {
          result = Iterator((e.srcId, e.attr), (e.dstId, e.attr))
        }
        result
      })
        .aggregateByKey(new Array[Int](nTopics + nDocTopics))(combineDeltaIntoCounts, combineCounts)

      graph = graph.outerJoinVertices(deltas)({ (vid, oldHistogram, vertexDeltasOption) =>
        if (vertexDeltasOption.isDefined) {
          val vertexDeltas = vertexDeltasOption.get
          // val parameter = if (vid < 0) a else b
          val histogram = applyDeltasToHistogram(oldHistogram, vertexDeltas, vid)
          histogram
        } else {
          Histogram(oldHistogram.counts, oldHistogram.docTopics)
        }
      }).cache()

      if (loggingTime && iteration % loggingInterval == 0 && iteration > 0) {
        updateCountsTimes.get += System.nanoTime() - tempTimer
      }

      // Recompute the global counts (the actual action)
      tempTimer = System.nanoTime()
      totalHistogram = Option(makeHistogramFromCounts(graph.edges.map(e => e.attr)
        .aggregate(new Array[Int](nTopics + nDocTopics))(combineTopicIntoCounts, combineCounts),
        Array()))
      assert(totalHistogram.get.counts.sum == nTokens)

      if (loggingTime && iteration % loggingInterval == 0 && iteration > 0) {
        globalCountsTimes.get += System.nanoTime() - tempTimer
      }

      iteration += 1

    }

    def optimizeParams(): Unit = {

    }

    /**
     * Creates an object holding the top counts. The first array is of size number
     * of topics. It contains a list of k elements representing the top words for that topic
 *
     * @param k Number of top words to output
     * @return object with top counts for each word.
     */
    def topWords(count: Int = 0): Array[Array[(Int, WordId)]] = {
      var k = count
      if (k < 1) k = posterior.words.count().toInt

      topicQueues.map(q => q.take(count).toArray)
    }

    def topicQueues: Array[PriorityQueue[(Int, WordId)]] = {
      val nt = nTopics + nDocTopics
      graph.vertices.filter({
        case (vid, c) => vid >= 0
      }).mapPartitions({ items =>
        // TODO: Make this a bounded queue
        val queues = Array.fill(nt)(new PriorityQueue[(Int, WordId)]())
        for ((wordId, factor) <- items) {
          var t = 0
          while (t < nt) {
            val tpl: (Int, WordId) = (factor.counts(t), wordId)
            queues(t) += tpl
            t += 1
          }
        }
        Iterator(queues)
      }).reduce({ (q1, q2) =>
        q1.zip(q2).foreach({ case (a, b) => a ++= b })
        q1
      })
    }

    def summarizeDocGroups(i: Int = 0): Array[(GroupId, DocId, Double)] = {
      val didVsSrc = graph.triplets.map(f => (f.dstId, f.srcAttr.counts)).groupBy(_._1)
      val docTriplets = graph.triplets.groupBy(f => (f.dstId))
      val res = docTriplets.join(didVsSrc)

      res.groupBy(_._2._1.head.dstAttr.docTopics(0)).map {
        case (docId, v) =>
          val repoId = v.head._2._1.head.dstAttr.docTopics(1)
          val m = v.flatMap {
            case (x, (y, z)) =>
              z.map(a => (a._2(0), a._2(1)))
          }
          val backgTopic = m.map(_._1).sum
          val repoTopic = m.map(_._2).sum
          val klScore = repoTopic.toDouble / (backgTopic + repoTopic)
          (repoId, docId * -1, klScore)
      }.collect()
    }


    /**
     * Creates the posterior distribution for sampling from the vertices
 *
     * @return Posterior distribution
     */
    def posterior: Posterior = {
      graph.cache()
      val words = graph.vertices.filter({ case (vid, _) => vid >= 0 })
      val docs = graph.vertices.filter({ case (vid, _) => vid < 0 })
      new Posterior(words, docs)
    }

    /**
     * Log likelihood of the model.
 *
     * @see H. M. Wallach, Structured Topic Models for Language
     */
 /*   def logLikelihood(): Double = {
      val nw = nWords
      val nt = nTopics
      val nd = nDocs
      val a = alpha
      val b = beta
      val totalHistogramCounts = totalHistogram.get.counts
      val logAlpha = Gamma.logGamma(a.sum)
      val sumBeta = Array.tabulate(totalHistogramCounts.length) { i =>
        totalHistogramCounts(i) * b(i)
      }.sum
      val sigmaLogAlphaK = a.map(f => Gamma.logGamma(f)).foldLeft(0.0)(_ + _)

      val logGamaBetaK = Array.tabulate(totalHistogramCounts.length) { i =>
        Gamma.logGamma(totalHistogramCounts(i) * b(i))
      }.sum
      val logPWGivenZ =
        nTopics * (Gamma.logGamma(sumBeta) - logGamaBetaK) -
          totalHistogramCounts.map(v => Gamma.logGamma(v + sumBeta)).sum +
          wordVertices.map({
            case (id, histogram) =>
              // histogram.counts.map(v => Gamma.logGamma(v + b(0))).sum
              Array.tabulate(histogram.counts.length) { i =>
                Gamma.logGamma(histogram.counts(i) + b(i))
              }.sum
          })
            .reduce(_ + _)
      val logPZ =
        nd * (logAlpha - sigmaLogAlphaK) +
          docVertices.map({
            case (id, histogram) =>
              val sigmaLogGamaNkPlusAlphaK = Array.tabulate(histogram.counts.length) { i =>
                Gamma.logGamma(histogram.counts(i) + a(i))
              }.sum
              val logGamaNdPlusAlpha = Gamma.logGamma(nt * a.sum + histogram.counts.sum)
              sigmaLogGamaNkPlusAlphaK - logGamaNdPlusAlpha
          }).reduce(_ + _)
      logPWGivenZ + logPZ
    }*/

    /**
     * Logs the final machine performance and ML performance to INFO
     */
    def logPerformanceStatistics(): Unit = {
      val resampleTime = if (loggingTime) resampleTimes.get.reduce(_ + _) / 1e9 else 0
      val updateCountsTime = if (loggingTime) updateCountsTimes.get.reduce(_ + _) / 1e9 else 0
      val globalCountsTime = if (loggingTime) globalCountsTimes.get.reduce(_ + _) / 1e9 else 0
      val likelihoodList = likelihoods.toList
      val likelihoodString = likelihoodList.mkString(",")
      val finalLikelihood = likelihoodList.last
      logInfo("LDA Model Parameters and Information")
      logInfo(s"Topics: $nTopics")
      logInfo(s"Alpha: $alpha")
      logInfo(s"Beta: $beta")
      logInfo(s"Number of Documents: $nDocs")
      logInfo(s"Number of Words: $nWords")
      logInfo(s"Number of Tokens: $nTokens")
      logInfo("Running Time Statistics")
      /* logInfo(s"Setup: $setupTime s")
      logInfo(s"Run: $runTime s")
      logInfo(s"Total: $totalTime s") */
      if (loggingTime) {
        logInfo(s"Resample Time: $resampleTime")
        logInfo(s"Update Counts Time: $updateCountsTime")
        logInfo(s"Global Counts Time: $globalCountsTime")
      }
      logInfo("Machine Learning Performance")
      if (loggingLikelihood) {
        logInfo(s"Likelihoods: $likelihoodString")
      }
      logInfo(s"Final Log Likelihood: $finalLikelihood")
    }

    /* private def setupTime: Double = {
      timer.getSeconds("setup")
    }
    private def runTime: Double = {
      timer.getSeconds("run")
    }
    private def totalTime: Double = {
      timer.getSeconds("setup") + timer.getSeconds("run")
    } */
    override def logLikelihood(): Double =  {
      2.3
    }
}
}
