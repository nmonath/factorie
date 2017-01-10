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


import java.util

import cc.factorie.DenseTensor1
import cc.factorie.util.{EvaluatableClustering, BasicEvaluatableClustering, DefaultCmdOptions}

import scala.collection.JavaConverters._
import scala.collection.mutable

trait DBSCANOpts extends DefaultCmdOptions {
  val minPoints = new CmdOption[Int]("min-points", "The minimum number of points for a region to be considered dense")
  val epsilon = new CmdOption[Double]("epsilon", "The radius of the ball drawn around points.")
}

/**
  * Outline of the implementation of DBSCAN
  * Allows for different distance functions to be used etc.
  * @tparam T
  * @tparam DP
  */
trait DBSCAN[T,DP <: DataPoint[T,DP]] extends ClusteringAlgorithm[DP] {

  override def predictedClustering: Iterable[(String, String)] = data.map(f => (f.id,f.cluster.id))

  override def getGoldLabel(dp: DP): Option[String] = dp.goldClusterID


  override def getId(dp: DP): String = dp.id

  /**
    * The data to be clustered
    * @return
    */
  override def data: IndexedSeq[DP]

  /**
    * The distance measure between the two points.
    * @return
    */
  def distance: (DP,DP) => Double

  /**
    * The distance function used in the clustering.
    * Note that having this separate from "distance"
    * allows flexibility for distances to be cached
    * or not depending on the application
    * @param p1
    * @param p2
    * @return
    */
  def distanceFunction(p1: DP, p2: DP): Double = distance(p1,p2)

  /**
    * Input parameter for DBScan. Radius of the ball placed around points
    * @return
    */
  def epsilon: Double

  /**
    * Number of points for the region to be considered dense.
    * @return
    */
  def minPts: Int

  /**
    * Runs the clustering algorithm
    */
  def cluster(): Unit = {

    var i = 0
    while (i < data.length) {
      // print(s"\r[DBSCAN] Processing point $i of ${data.length}")
      val currentPoint = data(i)
      if (!currentPoint.isMarked) {

        val withinEps = withinEpsOf(currentPoint)

        // Outlier
        if (withinEps.size < minPts) {
          currentPoint.setAsOutlier()
          currentPoint.mark()
          currentPoint.becomeSingleton()
        } else {
          expand(currentPoint)
        }
      }
      i += 1
    }
    // println()
  }

  /**
    * Find the data points that are within epsilon of the given point according to the distance function.
    * @param point
    * @return
    */
  def withinEpsOf(point: DP) = data.filter(p => distanceFunction(point,p) < epsilon)

  /**
    * The expand sub-rountine
    * @param focusPoint
    */
  def expand(focusPoint: DP) = {
    val pointsToVisit = new mutable.Queue[DP]()
    pointsToVisit.+=(focusPoint)
    while (pointsToVisit.nonEmpty) {
      val next = pointsToVisit.dequeue()
      if (!next.isMarked) {
        next.mark()
        val neighborsNeighbors = withinEpsOf(next)
        if (neighborsNeighbors.size >= minPts) {
          neighborsNeighbors.foreach {
            neighbor =>
              if (!neighbor.isMarked || neighbor.isOutlier)
                if (!neighbor.isMarked)
                  pointsToVisit.+=(neighbor)
              focusPoint.cluster.addPoint(neighbor)
          }
        }
      }
    }
  }

}

/**
  * Caches all of the distances between points
  * @tparam T
  * @tparam DP
  */
trait DistanceCachingDBSCAN[T,DP <: DataPoint[T,DP]] extends DBSCAN[T,DP] {

  protected val _distances = new util.HashMap[(DP,DP),Double]().asScala

  override def distanceFunction(p1: DP, p2: DP): Double = {
    if (!_distances.contains((p1,p2)) && !_distances.contains((p2,p1))) {
      _distances.put((p1, p2), super.distanceFunction(p1, p2))
      _distances((p1,p2))
    } else if (_distances.contains((p1,p2)))
      _distances((p1,p2))
    else
      _distances((p2,p1))
  }
}


/**
  * Caches all of the neighbors of points
  * @tparam T
  * @tparam DP
  */
trait NeighborCachingDBSCAN[T,DP<: DataPoint[T,DP]] extends DBSCAN[T,DP] {

  protected val _neighbors = new util.HashMap[DP,IndexedSeq[DP]]().asScala

  override def withinEpsOf(point: DP) = {
    if (_neighbors.contains(point))
      _neighbors(point)
    else {
      val res = data.filter(p => distanceFunction(point,p) < epsilon)
      _neighbors.put(point,res)
      res
    }
  }
}

/**
  * DBSCAN with both distances and neighbors cached.
  * @param data
  * @param epsilon
  * @param minPts
  * @param distance
  * @tparam T
  * @tparam DP
  */
class BasicDBSCAN[T,DP <: DataPoint[T,DP]](
                                            override val data: IndexedSeq[DP],
                                            override val epsilon: Double,
                                            override val minPts: Int,
                                            override val distance: (DP,DP) => Double
                                          ) extends DBSCAN[T,DP] with DistanceCachingDBSCAN[T,DP] with NeighborCachingDBSCAN[T,DP]


/**
  * DBScan for dense vectors using euclidean distance.
  * @param data
  * @param eps
  * @param minPts
  */
class EuclideanDistanceDBSCAN(data: IndexedSeq[VectorDataPoint], eps: Double, minPts: Int)
  extends BasicDBSCAN[DenseTensor1,VectorDataPoint](data,eps,minPts,(o,t) => o.datapoint.l2Similarity(t.datapoint))


object RunDBSCAN {
  def main(args: Array[String]): Unit = {
    val fn = args(0)
    val eps = args(1).toDouble
    val minPts = args(2).toInt
    val vectors = LoadVectorDataPoint.loadFile(fn)
    val dbscan = new EuclideanDistanceDBSCAN(vectors,eps,minPts)
    dbscan.cluster()
    val pred = new BasicEvaluatableClustering(dbscan.predictedClustering)
    val gold = new BasicEvaluatableClustering(dbscan.goldClustering)
    println(EvaluatableClustering.evaluationString(pred,gold))
  }
}