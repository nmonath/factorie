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
import java.util.UUID

import cc.factorie.la.DenseTensor1

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag


trait ClusteringAlgorithm[DataType] {

  val name = this.getClass.getName.replaceAll("\\$$","").split("\\.").last

  def data: IndexedSeq[DataType]

  def predictedClustering: Iterable[(String,String)]

  def cluster(): Unit

  def goldClustering: Iterable[(String,String)] = data.filter(d => getGoldLabel(d).isDefined).map(d => (getId(d),getGoldLabel(d).get))

  def getGoldLabel(dp: DataType): Option[String] = None
  def getId(dp: DataType): String = dp.toString
}

trait GraphvisLabel {
  def graphvisLabel: String = s"$toString"
}

abstract class DataPoint[T, DP <: DataPoint[T,DP]: ClassTag](val id: String = UUID.randomUUID().toString) extends GraphvisLabel {
  self : DP =>

  /**
    * The value of the object we are clustering.
    */
  val datapoint: T

  def pointID: String
  def goldLabel: String
  /**
    * Pointer to the cluster in which this data point sits.
    * TODO: How do we want to handle data where the clusters can't all sit in memory?
    */
  var cluster: Cluster[T,DP] = new Cluster[T,DP](Iterable(this))

  /**
    * Make this datapoint become a singleton, i.e. remove it from the cluster it is in
    * and start a new one.
    */
  def becomeSingleton() = {
    if (!cluster.isSingleton) {
      cluster.removePoint(this)
      cluster = new Cluster[T,DP](ArrayBuffer(this))
    }
  }

  var goldClusterID: Option[String] = None

  // For marking the data points
  protected var _marked = false
  def isMarked: Boolean = _marked
  def mark(): Unit = _marked = true
  def unMark(): Unit = _marked = false

  // For setting the data points as outliers
  protected var _outlier = false
  def isOutlier: Boolean = _outlier
  def setAsOutlier(): Unit = _outlier = true
  def removeAsOutlier(): Unit = _outlier = false

  def equalValue(other: DP): Boolean =
    this.datapoint equals other.datapoint

}

class Cluster[T,DataPointType <: DataPoint[T,DataPointType]](val points: mutable.Set[DataPointType], val id: String) {

  def this(points: mutable.Set[DataPointType]) = this(points,UUID.randomUUID().toString)

  def this() = this(new util.HashSet[DataPointType]().asScala)

  def this(pts: Iterable[DataPointType]) = {
    this()
    pts.foreach(this.points.add)
  }

  def isEmpty: Boolean = points.isEmpty

  def isSingleton: Boolean = points.size == 1

  def removePoint(p: DataPointType) = points remove  p

  def addPoint(p: DataPointType) = {
    p.cluster.removePoint(p)
    p.cluster = this
    points add p
  }

  def merge(other: Cluster[T,DataPointType]) = {
    other.points.foreach(p => {this.points.add(p); p.cluster = this})
    other.points.clear()
    this
  }

  var center: Option[DataPointType] = None
}


trait Addable[DP <: Addable[DP]] {
  self : DP =>

  def +(other: DP): DP
  def +=(other: DP): Unit
  def -(other: DP): DP
  def -=(other: DP): Unit
}


trait Multipliable[DP <: Multipliable[DP]] {
  self : DP =>

  def *(other: DP): DP
  def *=(other: DP): Unit
  def /=(other: DP): Unit

}

/**
  * A data point that is defined by a dense vector point.
  * @param datapoint
  */
class VectorDataPoint(override val datapoint: DenseTensor1, id: String = UUID.randomUUID().toString)
  extends DataPoint[DenseTensor1,VectorDataPoint](id) with Addable[VectorDataPoint] with Multipliable[VectorDataPoint] {

  override def toString = s"VectorDataPoint($id,${datapoint.mkString("[",",","]")})"

  override def +(other: VectorDataPoint): VectorDataPoint = new VectorDataPoint((this.datapoint + other.datapoint).asInstanceOf[DenseTensor1])

  override def -=(other: VectorDataPoint): Unit = this.datapoint -= other.datapoint

  override def +=(other: VectorDataPoint): Unit = this.datapoint += other.datapoint

  override def -(other: VectorDataPoint):VectorDataPoint = new VectorDataPoint((this.datapoint - other.datapoint).asInstanceOf[DenseTensor1])

  override def *=(other: VectorDataPoint): Unit = this.datapoint *= other.datapoint

  def / (n: Double): VectorDataPoint = new VectorDataPoint((this.datapoint / n).asInstanceOf[DenseTensor1])

  override def /=(other: VectorDataPoint): Unit = this.datapoint /= other.datapoint

  override def *(other: VectorDataPoint): VectorDataPoint = new VectorDataPoint(this.datapoint * other.datapoint)

  def dot(other: VectorDataPoint) = this.datapoint dot other.datapoint

  override def equalValue(other:VectorDataPoint): Boolean = {
    if (this.datapoint.length == other.datapoint.length) {
      var i = 0
      while (i < this.datapoint.length) {
        if (this.datapoint(i) != other.datapoint(i))
          return false
        i += 1
      }
      true
    } else {
      false
    }
  }

  override def pointID: String = id

  override def goldLabel: String = goldClusterID.get

}

object VectorDataPoint {

  def l2Distance = (one: VectorDataPoint, two:VectorDataPoint) => one.datapoint.l2Similarity(two.datapoint)
}