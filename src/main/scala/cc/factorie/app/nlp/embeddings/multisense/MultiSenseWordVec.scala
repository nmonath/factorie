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

import java.nio.charset.Charset

object MultiSenseWordVec {
  def main(args: Array[String]) {
    val opts = new MultiSenseSkipgramEmbeddingOpts {}
    opts.parse(args)
    println("Default Charset of this JVM =" + Charset.defaultCharset());
    println("User Provided Charset for this project=" + opts.encoding.value)

    val is_valid_model = opts.model.value.equals("MSSG-KMeans") || opts.model.value.equals("MSSG-MaxOut")
    if (is_valid_model) {
      println("Leanring Multiple Embeddings using " + opts.model.value)
      val wordEmbedding = new MultiSenseSkipGramEmbeddingModel(opts)
      val st1 = System.currentTimeMillis()
      wordEmbedding.buildVocab()
      val st = System.currentTimeMillis()
      println("time taken to build vocab : " + (st - st1) / 1000.0)
      wordEmbedding.learnEmbeddings()
      val en = System.currentTimeMillis() - st
      println("time taken to learn embedding : " + en / 1000.0)
      wordEmbedding.store()
    }
    else {
      println("InValid Model-" + opts.model.value)
      println("Model Options are - MSSG-KMeans, MSSG-MaxOut")
      return
    }

  }
}