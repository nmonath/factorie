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

import java.io.PrintWriter
import java.nio.charset.Charset

import cc.factorie.app.nlp.Document
import cc.factorie.app.nlp.segment.{DeterministicTokenizer, DeterministicSentenceSegmenter}

object preprocess {
  def main(args : Array[String]) {
      println("Default Charset=" + Charset.defaultCharset());
    	println("file.encoding=" + System.getProperty("file.encoding"));

      val inputFile = args(0)
      val outFile = args(1)
      val out = new PrintWriter(outFile, "ISO-8859-15")
      val tokenizer =  DeterministicTokenizer
      val sen_tokenizer = new DeterministicSentenceSegmenter
      for (line <- io.Source.fromFile(inputFile, "ISO-8859-15").getLines) {

            val doc = new Document(line)
            tokenizer.process(doc)
            sen_tokenizer.process(doc)
            if (doc.tokens.size > 10) {
              for (token <- doc.tokens)
                out.print(token.string + " ")
               out.print("\n")
               out.flush()
            }
            else {
              println(doc.tokens.toArray)
            }

      }
      out.close()
  }
}