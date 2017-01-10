/* Copyright (C) 2008-2016 University of Massachusetts Amherst.
   This file is part of "FACTORIE" (Factor graphs, Imperative, Extensible)
   http://factorie.cs.umass.edu, http://github.com/factorie
   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License. */

package cc.factorie.app.nlp.embeddings.multisense

import java.io.{BufferedOutputStream, File, FileInputStream, FileOutputStream, OutputStreamWriter}
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import cc.factorie.app.nlp.embeddings._
import cc.factorie.la.DenseTensor1
import cc.factorie.model.{Parameters, Weights}
import cc.factorie.optimize.AdaGradRDA
import cc.factorie.util.Threading


trait MultiSenseSkipgramEmbeddingOpts extends EmbeddingOpts {

  //
  // Model
  val model                   = new CmdOption("model", "MSSG-KMeans", "STRING", "use <string> name of the model")
  // Multiple Embeddings Option
  //
  val sense                   = new CmdOption("sense", 5, "INT", "use <int> senses")
  val bootVectors             = new CmdOption("boot-vectors", "", "STRING", "use <string> vector file")
  val updateGlobal            = new CmdOption("update-global", 1, "INT", "use 0 to not to update global vectors. works only with bootstrapping")
  //
  // Clustering (DP means)
  //
  val learnOnlyTop            = new CmdOption("learn-top-v", 4000, "INT", "use <double>")
  val createClusterLambda     = new CmdOption("create-cluster-lambda", 0.0, "DOUBLE", "use <int> to determine when to create the cluster")
  val loadMultiSenseVocabFile = new CmdOption("load-multi-vocab", "", "STRING", "feed 6000 vocab that socher used to learn multiple embeddings")
  val debeugClusterInfo       = new CmdOption("debug-cluster-info", "", "STRING", "use <debug> info")

}

abstract class MultiSenseWordEmbeddingModel(val opts: MultiSenseSkipgramEmbeddingOpts) extends Parameters {

  // Algorithm related
  val D                             = opts.dimension.value           // default value is 200
  val S                             = opts.sense.value               // default value is 5
  var V                             = 0                              // vocab size. Will computed in buildVocab() section
  protected val minCount            = opts.minCount.value            // default value is 5
  protected val ignoreStopWords     = opts.ignoreStopWords.value     // default value is 0
  protected val vocabHashSize       = opts.vocabHashSize.value       // default value is 20 M
  protected val maxVocabSize        = opts.vocabSize.value           // default value is 1M
  protected val samplingTableSize   = opts.samplingTableSize.value   // default value is 100 M
  val updateGlobal                  = if (opts.bootVectors.value.size == 0) 1 else opts.updateGlobal.value.toInt
  // Optimization Related
  protected val threads             = opts.threads.value             // default value is 12
  protected val adaGradDelta        = opts.delta.value               // default value is 0.1
  protected val adaGradRate         = opts.rate.value                // default value is 0.025
  // IO Related
  val corpus                        = opts.corpus.value              //
  val storeInBinary                 = opts.binary.value              // default is true
  val loadVocabFilename             = opts.loadVocabFile.value
  val saveVocabFilename             = opts.saveVocabFile.value
  protected val outputFile          = opts.output.value
  private val encoding              = opts.encoding.value            // default is UTF-8
  // Sense/Cluster related
  protected val createClusterlambda = opts.createClusterLambda.value // when to create a new cluster
  val kmeans                        = if (opts.model.value.equals("MSSG-KMeans"))      1 else 0
  val maxout                        = if (opts.model.value.equals("MSSG-MaxOut"))      1 else 0
  protected var multiVocabFile      = opts.loadMultiSenseVocabFile.value

  // embedding data structures
  protected var vocab: VocabBuilder           = null
  protected var trainer: LiteHogwildTrainer   = null
  protected var optimizer: AdaGradRDA         = null
  private var corpusLineItr: Iterator[String] = null
  private var train_words: Long               = 0

  // embeddings - global_weights contain the global embeddings(context) and sense_weights contain the embeddings for every word
  // w.r.t to that sense
  var sense_weights: Seq[Seq[Weights]]        = null
  var global_weights: Seq[Weights]            = null

  // clustering related data structures
  protected var clusterCenter: Array[Array[DenseTensor1]] = null
  protected var clusterCount: Array[Array[Int]]           = null
  protected var clusterActive: Array[Array[Int]] = null
  protected var ncluster: Array[Int] = null // holds the information for # of cluster
  protected var countSenses: Array[Int] = null
  protected var learnMultiVec: Array[Boolean] = null

  // Component-1
  def buildVocab(minFreq: Int = 5): Unit = {
    vocab = new VocabBuilder(vocabHashSize, samplingTableSize, 0.7) // 0.7 is the load factor
    println("Building Vocab")
    if (loadVocabFilename.size == 0) {
      val corpusLineItr = corpus.endsWith(".gz") match {
        case true  => io.Source.fromInputStream(new GZIPInputStream(new FileInputStream(corpus)), encoding).getLines
        case false => io.Source.fromInputStream(new FileInputStream(corpus), encoding).getLines
      }
      while (corpusLineItr.hasNext) {
       val line = corpusLineItr.next
       line.stripLineEnd.split(' ').foreach(word => vocab.addWordToVocab(word.toLowerCase()))
      }
    }
    else vocab.loadVocab(loadVocabFilename, encoding)

    vocab.sortVocab(minCount, if (ignoreStopWords) 1 else 0, maxVocabSize)  // removes words whose count is less than minCount and sorts by frequency
    vocab.buildSamplingTable() // for getting random word from vocab in O(1) otherwise would O(log |V|)
    vocab.buildSubSamplingTable(opts.sample.value)  // pre-compute subsampling table
    V = vocab.size()
    train_words = vocab.trainWords()
    println("Corpus Stat - Vocab Size :" + V + " Total words (effective) in corpus : " + train_words)
    // save the vocab if the user provides the filename save-vocab
    if (saveVocabFilename.size != 0) {
      println("Saving Vocab into " + saveVocabFilename)
      vocab.saveVocab(saveVocabFilename, if (storeInBinary) 1 else 0, encoding) // for every word, <word><space><count><newline>
      println("Done Saving Vocab")
    }

    learnMultiVec = Array.fill[Boolean](V)(false)
    // load a specific vocab-file for learning multiple-embeddings if learnTopV is 0
    if (multiVocabFile.size != 0) {
        println("Learning multiple embeddings only for those words in " + multiVocabFile)
        for (line <- io.Source.fromFile(multiVocabFile).getLines()) {
          val wrd = line.stripLineEnd
          val id  = vocab.getId(wrd)
          assert(id != -1)
          learnMultiVec(id) = true
        }
        println("Done Loading the socher-multi-vocab-file")
     } else {
        val learnTopV = math.min(opts.learnOnlyTop.value, V)
        println("learn-top-v " + learnTopV)
        println("Learning multiple embeddings for the top most frequent " + learnTopV + " words. ")
        for (v <- 0 until learnTopV)
          learnMultiVec(v) = true
     }
  }

  // Component-2
  def learnEmbeddings(): Unit = {
    println("Learning Embeddings")

    clusterCount  = Array.ofDim[Int](V, S)
    clusterCenter = Array.ofDim[DenseTensor1](V, S)
    ncluster = Array.ofDim[Int](V)
    for (v <- 0 until V) {
      ncluster(v) = if (learnMultiVec(v)) S else 1
      for (s <- 0 until S) {
        clusterCount(v)(s)  = 1
        clusterCenter(v)(s) = TensorUtils.setToRandom1(new DenseTensor1(D, 0))
      }
    }
    optimizer     = new AdaGradRDA(delta = adaGradDelta, rate = adaGradRate)
    // initialized to random (same manner as in word2vec)
    sense_weights = (0 until V).map(v => (0 until ncluster(v)).map(s => Weights(TensorUtils.setToRandom1(new DenseTensor1(D, 0)))))
    if (!opts.bootVectors.value.equals(""))
      load_weights()
    else
      global_weights = (0 until V).map(i => Weights(TensorUtils.setToRandom1(new DenseTensor1(D, 0))))
    optimizer.initializeWeights(this.parameters)
    trainer = new LiteHogwildTrainer(weightsSet = this.parameters, optimizer = optimizer, nThreads = threads, maxIterations = Int.MaxValue)

    println("Initialized Parameters: ")
    println(println("Total memory available to JVM (bytes): " +
        Runtime.getRuntime().totalMemory()))

    val files = (0 until threads).map(i => i)
    Threading.parForeach(files, threads)(workerThread(_))
    println("Done learning embeddings. ")
  }

  def load_weights():Unit = {
   val bootEmbeddingsFile = opts.bootVectors.value
   val bootLineItr = opts.bootVectors.value.endsWith(".gz") match {
      case false => io.Source.fromFile(bootEmbeddingsFile, encoding).getLines
      case true => io.Source.fromInputStream(new GZIPInputStream(new FileInputStream(bootEmbeddingsFile)), encoding).getLines
    }
    val details = bootLineItr.next.stripLineEnd.split(' ').map(_.toInt)
    val Vcheck  = details(0)
    val Dcheck  = details(1)
    assert(Vcheck == V && Dcheck == D)
    println("Bootstrappng vectors : # words : %d , # size : %d".format(V, D))

    val vectors = new Array[DenseTensor1](V)
    for (v <- 0 until V) {
      val line = bootLineItr.next.stripLineEnd.split(' ')
      assert(line.size == 1)
      val word = line(0)
      val org_word = vocab.getWord(v)
      assert(word.equals(org_word) )
      val fields = bootLineItr.next.stripLineEnd.split("\\s+")
        vectors(v) = new DenseTensor1(fields.map(_.toDouble))
     }
     global_weights = (0 until V).map(i => Weights(vectors(i))) // initialized using wordvec random
  }

  // Component-3
  def store(): Unit = {
     println("Now, storing into output... ")
     val out = storeInBinary match {
      case false => new java.io.PrintWriter(outputFile, encoding)
      case true => new OutputStreamWriter(new GZIPOutputStream(new BufferedOutputStream(new FileOutputStream(outputFile))), encoding)
    }
     out.write("%d %d %d %d\n".format(V, D, S, maxout))
     out.flush()
     for (v <- 0 until V) {
       val C = if (learnMultiVec(v)) ncluster(v) else 1
       out.write(vocab.getWord(v) + " " + C)
       //for (s <- 0 until C) out.write(" " + clusterCount(v)(s))
       out.write("\n"); out.flush();
       val global_embedding = global_weights(v).value
       for (d <- 0 until D) {
         out.write(global_embedding(d) + " ")
       }
       out.write("\n"); out.flush()
       for (s <- 0 until C) {
         val sense_embedding = sense_weights(v)(s).value
         for (d <- 0 until D) {
           out.write(sense_embedding(d) + " ")
         }
         out.write("\n"); out.flush()
        // if maxout method is not used
        if (maxout == 0) {
          val mu = clusterCenter(v)(s) / (1.0 * clusterCount(v)(s))
          for (d <- 0 until D) {
            out.write(mu(d) + " ")
          }
          out.write("\n"); out.flush()
        }
      }
     }
     out.close()
     println("Done, Storing")
  }

  protected def workerThread(id: Int): Unit = {
    val fileLen = new File(corpus).length
    val skipBytes: Long = fileLen / threads * id // skip bytes. skipped bytes is done by other workers
    val lineItr = new FastLineReader(corpus, skipBytes)
    var word_count: Long = 0
    var work = true
    var ndoc = 0
    // worker amount
    val total_words_per_thread = train_words / threads
    while (lineItr.hasNext && work) {
      word_count += process(lineItr.next)
      ndoc += 1
      // print the progress after processing 50 docs (or lines) for Thread-1
      // Approximately reflects the progress for the entire system as all the threads are scheduled "fairly"
      if (id == 1 && ndoc % 50 == 0) {
        val progress = math.min( (word_count/total_words_per_thread.toFloat) * 100, 100.0)
        println(f"Progress: $progress%2.2f" + " %")
      }
      // Once, word_count reaches this limit, ask worker to end
      if (word_count > total_words_per_thread) work = false
    }
  }

  // Override this function in your Embedding Model like SkipGramEmbedding or CBOWEmbedding
  protected def process(doc: String): Int

}
