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
import cc.factorie.la.DenseTensor1
import cc.factorie.util.{EvaluatableClustering, BasicEvaluatableClustering}
import scala.util.Random

class KMeansCenter(val initialMean: DenseTensor1) extends Cluster[DenseTensor1,VectorDataPoint] {
  var mean = initialMean.copy
  val dim = initialMean.dim1
  def updateMean() = {
    mean = new DenseTensor1(dim,0.0)
    points.foreach(m => mean.+=(m.datapoint))
    if (points.nonEmpty)
      mean /= points.size
  }
  def forgetPoints() = points.clear()
}
class KMeans(val dataPoints: IndexedSeq[VectorDataPoint], val k: Int, val dim: Int, val numIterations: Int = 20) extends ClusteringAlgorithm[VectorDataPoint]{

  val clusterCenters = new scala.collection.mutable.HashSet[KMeansCenter]
  val shuffleData = new Random().shuffle(dataPoints)
  def initalize(): Unit = {
    val samples = shuffleData.take(k)
    samples.foreach {
      s =>
        clusterCenters += new KMeansCenter(s.datapoint.copy)
    }
  }

  def distance(dataPoints1: DenseTensor1, dataPoints2: DenseTensor1): Double = {
    dataPoints1.l2Similarity(dataPoints2)
  }


  def assignCluster() = {
    dataPoints.foreach {
      d =>
        var min = Double.PositiveInfinity
        var minCluster: KMeansCenter = null
        clusterCenters.foreach {
          center =>
            val tempMin = distance(d.datapoint, center.mean)
            val tempCluster = center
            if (tempMin < min) {
              min = tempMin
              minCluster = tempCluster
            }
        }
        minCluster addPoint  d
    }
  }

  def updateStep() = {
    clusterCenters.foreach(_.updateMean())
    clusterCenters.foreach(_.forgetPoints())
  }

  def cluster() = {
    initalize()
    (0 until numIterations).foreach {
      iter =>
        assignCluster()
        updateStep()
    }
    assignCluster()
  }

  override val name: String = "Kmeans"

  override def data: IndexedSeq[VectorDataPoint] = dataPoints

  override def predictedClustering: Iterable[(String, String)] = clusterCenters.flatMap{
    case c =>
      c.points.map{
        p =>
          (p.id,c.id)
      }
  }

  override def getGoldLabel(dp: VectorDataPoint): Option[String] = dp.goldClusterID

  override def getId(dp: VectorDataPoint): String = dp.id
}


object RunKmeans {

  def main(args: Array[String]): Unit= {
    val fn = args(0)
    val k = args(1).toInt
    val vectors = LoadVectorDataPoint.loadFile(fn)
    val kmeans = new KMeans(vectors,k,vectors.head.datapoint.dim1)
    kmeans.cluster()
    val pred = new BasicEvaluatableClustering(kmeans.predictedClustering)
    val gold = new BasicEvaluatableClustering(kmeans.goldClustering)
    println(EvaluatableClustering.evaluationString(pred,gold))
  }
}