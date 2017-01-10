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

package cc.factorie.app.clustering

import java.io.File

import cc.factorie.la.DenseTensor1


/**
  * Format:
  * cluster label \t tab separated vector
  */
object LoadVectorDataPoint {

  def loadLine(line: String) = {
    val splt = line.split("\t")
    val label = splt.head
    val vec = new DenseTensor1(splt.drop(1).map(_.toDouble))
    (label,vec)
  }

  def loadFileTuples(file: File): IndexedSeq[(String,DenseTensor1)] = scala.io.Source.fromFile(file).getLines().map(loadLine).toIndexedSeq

  def loadFileTuples(filename: String): IndexedSeq[(String,DenseTensor1)] = loadFileTuples(new File(filename))

  def loadFile(file: File): IndexedSeq[VectorDataPoint] = loadFileTuples(file).map{
    t =>
      val v = new VectorDataPoint(t._2)
      v.goldClusterID = Some(t._1)
      v
  }
  def loadFile(filename: String): IndexedSeq[VectorDataPoint] = loadFile(new File(filename))

}
