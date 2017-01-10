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

import cc.factorie._
import cc.factorie.util.{EvaluatableClustering, BasicEvaluatableClustering, DefaultCmdOptions}

import scala.collection.JavaConverters._

trait BirchClustering[T <: GraphvisLabel, EntryType <: BirchTreeNodeEntry[T ,EntryType,NodeType], NodeType <: BirchTreeNode[T,EntryType,NodeType]] {

  def singletonEntryConstructor: (T) => EntryType

  def emptyEntryConstructor: () => EntryType

  def emptyNodeConstructor: () => NodeType

  var root: NodeType = {
    val r = emptyNodeConstructor()
    r
  }

  def cluster(data: Iterable[T]) = {
    data.zipWithIndex.foreach{case(d,idx) => {
      // print(s"\rProcessed $idx/${data.size} points")
      val mention = singletonEntryConstructor(d)
      mention.setAsMention()
      root.insert(mention, emptyNodeConstructor, emptyEntryConstructor)
      if (root.parentNode.isDefined)
        root = root.parentNode.get
    }}
  }


  def internalNodeColorwheel = IndexedSeq("blue", "royalblue", "skyblue", "seagreen", "palegreen", "powderblue", "purple", "plum", "seagreen", "seagreen1", "lawngreen", "turquoise")

  def getColor(i: Int) = internalNodeColorwheel(i % internalNodeColorwheel.length)


  def toGraphvis(numDataPointsToShow: Int = 15): String = {
    val sb = new StringBuilder(10000)
    sb append s"digraph BirchTreeStructure {\n"
    sb append formatGraphvisNode(root,numDataPointsToShow)
    sb append "\n"
    sb append "}"
    sb.toString()
  }

  def cleanEntityIds(string: String) = "id" + string.replaceAll("[\\-\\.#]", "")

  def formatGraphvisEntry(entry: EntryType) = {
    s"\n${cleanEntityIds(entry.nodeId)}[shape=invhouse;style=bold;  color = ${getColor(entry.outerNode.get.depth % internalNodeColorwheel.size)}; label =  <${entry.graphvisLabel}>];"
  }

  def formatGraphvisNode(node: NodeType) = {
    s"\n${cleanEntityIds(node.nodeId)}[shape=rect;style=bold;  color = ${getColor(node.depth % internalNodeColorwheel.size)}; label =  <${node.graphvisLabel}>];"
  }

  def formatEntryNodeEdge(entry: EntryType) = {
    s"\n${cleanEntityIds(entry.outerNode.get.nodeId)}->${cleanEntityIds(entry.nodeId)}"
  }

  def formatNodeEntryEdge(node: NodeType) = {
    if (node.parent.isDefined)
      s"\n${cleanEntityIds(node.parent.get.nodeId)}->${cleanEntityIds(node.nodeId)}"
    else
      ""
  }

  def getPointID(pt:T): String

  def formatGraphvisPt(pt: T, entry: EntryType) = {
    s"\n${cleanEntityIds(getPointID(pt))}[shape=egg;style=bold;  color = ${getColor(entry.outerNode.get.depth % internalNodeColorwheel.size)}; label =  <${pt.graphvisLabel}>];" +
      s"\n${cleanEntityIds(entry.nodeId)}->${cleanEntityIds(getPointID(pt))}"
  }

  def formatMoreThanNPts(entry: EntryType,n : Int) = {
    val id = "id" + entry.nodeId + s"plus${n}more"
    s"\n$id[shape=egg;style=bold;  color = ${getColor(entry.outerNode.get.depth % internalNodeColorwheel.size)}; label = <And $n More>];" +
      s"\n${cleanEntityIds(entry.nodeId)}->$id"
  }

  def formatGraphvisSubtree(entry: EntryType,numChildPts: Int = 15): String = {
    val res = new StringBuffer(1000)
    res append formatGraphvisEntry(entry)
    res append formatEntryNodeEdge(entry)
    // if entry is leaf
    if (entry.child.isEmpty) {
      val dataPoints = entry.points.take(numChildPts)
      val dpString = dataPoints.map(formatGraphvisPt(_,entry))
      res append dpString.mkString("\n")
      if (dataPoints.size == numChildPts) {
        res append formatMoreThanNPts(entry, entry.numPoints - numChildPts)
      }
      res.toString
    } else {
      res append formatGraphvisNode(entry.child.get,numChildPts)
      res.toString
    }
  }

  def formatGraphvisNode(node: NodeType, numChildPts: Int): String = {
    // For a node, we want to
    // format the node object in the graph
    // add a link from child to parent
    val res = new StringBuffer(1000)
    res append formatGraphvisNode(node)
    res append formatNodeEntryEdge(node)
    node.entries.foreach{
      entry =>
        res append formatGraphvisSubtree(entry,numChildPts)
    }
    res.toString
  }


  def formatStringForEntry(entry: EntryType) = {
    s"entry\t${entry.outerNode.get.depth}\t${entry.outerNode.get.nodeId}\t${entry.nodeId}\t${entry.numPoints}\t${entry.radius}\t${entry.threshold}\t${entry.child.map(_.nodeId).getOrElse("None")}\t${entry.outerNode.get.parent.map(_.nodeId).getOrElse("None")}\t${entry.value.toString}"
  }

  def formatStringNode(node:NodeType) = {
    s"node\t${node.depth}\t${node.nodeId}\t${node.entries.size}\t${node.entries.map(_.nodeId).mkString(",")}\t${node.parent.map(_.nodeId).getOrElse("None")}"
  }

  def formatEntryTextFile = {
    val sb = new StringBuilder()
    sb append "#key\tdepth\tnode_id\tentry_id\tnum_points\tradius\tthreshold\tchild_node_id\touter_node_parent_id\tvalue\n"
    root.entries.foreach(e => sb.append(formatStringForEntry(e) + "\n"))
    root.descendants.foreach{
      node =>
        node.entries.foreach(e => sb.append(formatStringForEntry(e) + "\n"))
    }
    sb.toString()
  }

  def formatNodeTextFile = {
    val sb  = new StringBuilder()
    sb append "#key\tdepth\tnode_id\tnum_entries\tentry_ids\tparent_entry_id\n"
    sb append formatStringNode(root)
    sb append "\n"
    root.descendants.foreach{
      node =>
        sb append formatStringNode(node)
        sb append "\n"
    }
    sb.toString()
  }

}

trait BirchOpts extends DefaultCmdOptions {
  val threshold = new CmdOption[Double]("threshold","The threshold value")
  val branchingFactor = new CmdOption[Int]("branching-factor", "The branching factor")
  val input = new CmdOption[String]("input", "The input data file")
  val outputDir = new CmdOption[String]("output-dir","The output directory")
  val algorithmName = new CmdOption[String]("algorithm-name","The name of the algorithm to use")
}

import scala.collection.generic.{Growable, Shrinkable}
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

trait BirchTreeNodeEntry[T <: GraphvisLabel, EntryType <: BirchTreeNodeEntry[T,EntryType,NodeType], NodeType <: BirchTreeNode[T,EntryType,NodeType]] extends GraphvisLabel{

  self: EntryType =>

  val nodeId: String = math.abs(new Random().nextLong()).toString

  def getGoldLabels = points.flatMap{ _ match {
    case dp: DataPoint[T,_] =>
      dp.goldClusterID
    case _ =>
      None
  }
  }.groupBy(f => f).mapValues(_.size)

  override def graphvisLabel = s"$nodeId<BR/>NumPts:$numPoints<BR/>Radius:$radius<BR/>Threshold:$threshold<BR/>Value:${value.graphvisLabel}<BR/>GoldLabels:${getGoldLabels.map(f => f._1 + " x" +f._2).mkString(", ")}"

  def numPoints: Int

  /**
    * This is the tree node in which this item sits
    * @return
    */
  var outerNode: Option[NodeType] = None

  /**
    * This is the child node to which this item points.
    * @return
    */
  var child: Option[NodeType] = None

  /**
    * This is the value contained by the item
    * @return
    */
  def value: T

  def absorb(newEntry: EntryType):Unit = {
    absorbedEntries += newEntry
  }
  def unabsorb(newEntry: EntryType):Unit = {
    absorbedEntries -= newEntry
  }
  def canAbsorb(newEntry: EntryType) : Boolean = {
    absorb(newEntry)
    val rad = radius
    val res = rad <= threshold
    unabsorb(newEntry)
    res
  }

  def radius: Double

  def threshold: Double

  def centroid: T

  /**
    * All of the data points that stored in this cluster.
    * If you are using aggregate stats, just ignore this
    * @return
    */
  val points: ArrayBuffer[T] = new ArrayBuffer[T]()

  var isMention: Boolean = false

  def setAsMention(): Unit = isMention = true

  val absorbedEntries: Iterable[EntryType] with Growable[EntryType] with Shrinkable[EntryType] = new java.util.HashSet[EntryType]().asScala


  // We need to have the distance function here as well because there are certain
  // situations when merging/splitting that we don't have an outerNode and so wouldn't
  // otherwise have access to a distance function
  def distance(one: T, two: T): Double


}


/**
  * Tree nodes are a collection of [ entry 1 | entry 2 | ... ]
  * @tparam T
  */
trait BirchTreeNode[T <: GraphvisLabel, EntryType <: BirchTreeNodeEntry[T,EntryType, NodeType], NodeType <: BirchTreeNode[T,EntryType,NodeType]] extends GraphvisLabel{

  self : NodeType =>

  val nodeId: String = math.abs(new Random().nextLong()).toString

  override def graphvisLabel = s"$nodeId"

  /**
    * The entries that this tree node contains
    * @return
    */
  val entries = new java.util.HashSet[EntryType](branchingFactor + 1).asScala

  /**
    * This is the entry pointing to this node
    * @return
    */
  var parent: Option[EntryType] = None

  /**
    * This is the node which contains the parent
    * @return
    */
  def parentNode: Option[NodeType] = parent.flatMap(_.outerNode)

  /**
    * The children of this node in the tree
    * @return
    */
  def children: Iterable[NodeType] = entries.flatMap(_.child)

  def descendants: Iterable[NodeType] = if (this.isLeaf) Iterable() else children ++ children.flatMap(_.descendants)

  def descendantEntries = descendants.flatMap(_.entries)

  def mentions = descendantEntries.filter(_.isMention)

  def distance(one: T, two: T): Double

  /**
    *
    * @param othernode
    * @return
    */
  def entryClosestTo(othernode: EntryType): Option[EntryType] = {
    var minDist = Double.MaxValue
    var argMin: Option[EntryType] = None
    assert(entries.nonEmpty, "can only find the closest entry if entries is non empty")
    entries.foreach{
      e =>
        val dist = distance(e.centroid,othernode.centroid)
        if (dist < minDist) {
          minDist = dist
          argMin = Some(e)
        }
    }
    argMin
  }

  def isLeaf: Boolean = children.isEmpty

  def size: Int = entries.size

  def sizeLimit: Int = branchingFactor

  def hasSpace: Boolean = size < sizeLimit

  def threshold: Double

  def branchingFactor: Int

  def findAppropriateLeaf(newEntry: EntryType): Option[EntryType] = {
    if (isLeaf)
      entryClosestTo(newEntry)
    else
      entryClosestTo(newEntry).flatMap(_.child.flatMap(_.findAppropriateLeaf(newEntry)))
  }


  def insert(newEntry: EntryType, nodeConstructor: () => NodeType, entryConstructor: () => EntryType) = {
    //println(s"[BIRCH] Inserting ${newEntry.toString}")
    if (entries.isEmpty) {
      addEntryTo(newEntry, this, nodeConstructor, entryConstructor)
    } else {
      // Find where to add the guy
      // Then call the addEntryTo
      val whereToAdd = findAppropriateLeaf(newEntry)
      assert(whereToAdd.nonEmpty, "If there is an entry node, there should be somewhere to add this new entry")
      if (whereToAdd.get.canAbsorb(newEntry)) {
        whereToAdd.get.absorb(newEntry)
        whereToAdd.get.outerNode.get.propagateAbsorb(newEntry)
        whereToAdd.get.absorbedEntries += newEntry
        newEntry.outerNode = whereToAdd.get.outerNode
      } else {
        addEntryTo(newEntry, whereToAdd.get.outerNode.get, nodeConstructor, entryConstructor)
      }
    }
  }


  def addEntryTo(newEntry: EntryType, addTo: NodeType, nodeConstructor: () => NodeType, entryConstructor: () => EntryType): Unit = {

    //println(s"[BIRCH] Adding ${newEntry.toString} to ${addTo.toString}")

    if (addTo.hasSpace) {
      //println(s"[BIRCH] Creating new entry for ${newEntry.toString} in ${addTo.toString}")
      addTo.entries += newEntry
      newEntry.outerNode = Some(addTo)
      addTo.propagateAbsorb(newEntry)
    } else {
      // split procedure
      // Remove all the information about the entries in this
      // entry from the parent nodes to prepare for the deletion
      // split up the entries in this node
      // into two new nodes.
      // add the new nodes to the tree and propagate upward.

      //println(s"[BIRCH] No room for ${newEntry.toString} in ${addTo.toString}")
      //println(s"Splitting.")
      // Remove all info about these entries
      addTo.entries.foreach(addTo.propagateUnabsorb)

      // Split up the entries
      addTo.entries += newEntry
      val (l,r) = addTo.divideChildren
      addTo.entries.clear()
      //println(s"[BIRCH] Divided children into:")
      //println(s"\t1)${l.mkString("[",", ","]")}")
      //println(s"\t2)${r.mkString("[",", ","]")}")

      //println(s"[BIRCH] Entries in (1) added back to ${addTo.toString}")

      // For convenience, add the left group back to the
      // existing node. Then update the parent pointers
      addTo.entries ++= l
      addTo.entries.foreach(addTo.propagateAbsorb)
      l.foreach(_.outerNode = Some(addTo))

      // Create a new node.
      // Add the entries to it.
      val newNode = nodeConstructor()
      newNode.entries ++= r
      r.foreach(_.outerNode = Some(newNode))
      //println(s"[BIRCH] Created new node ${newNode.toString} containing (2)")

      // Create the entry which will be this new nodes parent
      val entryNewNodeParent = entryConstructor()
      r.foreach(entryNewNodeParent.absorb)
      entryNewNodeParent.child = Some(newNode)
      newNode.parent = Some(entryNewNodeParent)
      //println(s"[BIRCH] Created new entry ${entryNewNodeParent.toString} parenting ${newNode.toString}")


      // If we are not in the root node, just worry about updating this new guy
      if (addTo.parentNode.isDefined) {
        //println(s"[BIRCH] Inserting this new entry ${entryNewNodeParent.toString} into the parent node ${addTo.parentNode.get.toString}")
        addEntryTo(entryNewNodeParent,addTo.parentNode.get,nodeConstructor,entryConstructor)
      } else {
        // if we are in the root, when we split, we need to create a new root
        // note that for now it will be unbalanced....
        //println(s"[BIRCH] The split node ${addTo.toString} is the root. Creating a new root and entry in the root for this node.")
        val newRoot = nodeConstructor()
        val entryExistingNodeParent = entryConstructor()
        addTo.entries.foreach(entryExistingNodeParent.absorb)
        entryExistingNodeParent.child = Some(addTo)
        entryExistingNodeParent.outerNode = Some(newRoot)
        entryNewNodeParent.outerNode = Some(newRoot)
        addTo.parent = Some(entryExistingNodeParent)
        addEntryTo(entryExistingNodeParent,newRoot,nodeConstructor,entryConstructor)
        addEntryTo(entryNewNodeParent,newRoot,nodeConstructor,entryConstructor)
      }
    }
  }

  def propagateUnabsorb(entry: EntryType): Unit = {
    if (this.parent.isDefined) {
      this.parent.get.unabsorb(entry)
      this.parentNode.get.propagateUnabsorb(entry)
    }
  }

  def propagateAbsorb(entry: EntryType): Unit = {
    if (this.parent.isDefined) {
      this.parent.get.absorb(entry)
      this.parentNode.get.propagateAbsorb(entry)
    }
  }

  def divideChildren: (Seq[EntryType], Seq[EntryType]) = {
    val farthest = entries.pairs.maxBy(f => distance(f._1.centroid,f._2.centroid))
    val repOne = farthest._1
    val repTwo = farthest._2
    // println(s"rep1 pre2 distance ${distance(repOne.centroid,repTwo.centroid)}")
    val groupOne = new ArrayBuffer[EntryType](entries.size)
    groupOne += repOne
    val groupTwo = new ArrayBuffer[EntryType](entries.size)
    groupTwo += repTwo
    entries.foreach{
      node =>
        if (node != repOne && node != repTwo) {
          val distToRepOne = distance(node.centroid, repOne.centroid)
          assert(!distToRepOne.isNaN, "Somethings gone wrong, distance is NaN")
          val distToRepTwo = distance(node.centroid, repTwo.centroid)
          assert(!distToRepTwo.isNaN, "Somethings gone wrong, distance is NaN")
          if (distToRepOne <= distToRepTwo)
            groupOne += node
          else
            groupTwo += node
        }
    }
    (groupOne, groupTwo)
  }

  def leaves: Iterable[NodeType] =
    if (isLeaf)
      Iterable(this)
    else
      this.children.flatMap(_.leaves)

  def entriesOfLeaves = leaves.flatMap(_.entries)

  def depth = {
    var d = 0
    var cur = this
    while (cur.parent.isDefined) {
      cur = cur.parent.get.outerNode.get
      d += 1
    }
    d
  }

  def removeEntryAndPropagate(entry: EntryType) = {
    entries -= entry
    deleteEmptyNodesRecursively()
  }

  def deleteEmptyNodesRecursively(): Unit = {
    if (entries.isEmpty && parent.isDefined) {
      val par = parent.get
      par.outerNode.get.entries -= par
      par.outerNode.get.deleteEmptyNodesRecursively()
    }
  }

}


trait VectorTreeNodeEntry[EntryType <: VectorTreeNodeEntry[EntryType,NodeType], NodeType <: VectorTreeNode[EntryType,NodeType]] extends BirchTreeNodeEntry[VectorDataPoint,EntryType,NodeType] {
  self: EntryType =>
  var sumSq: Double = value.datapoint.sumSq()

  var numPoints: Int

  override def toString = s"VectorTreeNodeEntry($numPoints,${value.toString},$sumSq,$radius)"

  override def distance(one: VectorDataPoint, two: VectorDataPoint): Double = VectorDataPoint.l2Distance(one,two)

  override def absorb(newEntry: EntryType): Unit = {
    super.absorb(newEntry)
    //println(s"[BIRCH] ${this.toString} absorbing ${newEntry.toString}")
    numPoints += newEntry.numPoints
    value += newEntry.value
    sumSq += newEntry.sumSq
    points ++= newEntry.points
  }

  def absorb(vdp: VectorDataPoint) = {
    numPoints += 1
    value += vdp
    sumSq += vdp.datapoint.sumSq()
    points += vdp
  }

  override def unabsorb(newEntry: EntryType): Unit = {
    super.unabsorb(newEntry)
    //println(s"[BIRCH] ${this.toString} unabsorbing ${newEntry.toString}")
    numPoints -= newEntry.numPoints
    value -= newEntry.value
    sumSq -= newEntry.sumSq
    points --= newEntry.points
  }

  def unabsorb(vdp: VectorDataPoint) = {
    numPoints -= 1
    value -= vdp
    sumSq -= vdp.datapoint.sumSq()
    points -= vdp
  }

  override def centroid: VectorDataPoint = if (numPoints == 0) value else value / numPoints

  override def radius: Double = {
    if (numPoints == 0)
      0.0
    else {
      val mu = centroid
      val muSumSq = mu.datapoint.sumSq()
      math.sqrt(((sumSq - 2.0 * (value dot mu)) / numPoints) + muSumSq)
    }
  }
}

class BasicVectorTreeNodeEntry(override val value: VectorDataPoint, override val threshold: Double, var numPoints: Int) extends VectorTreeNodeEntry[BasicVectorTreeNodeEntry,BasicVectorTreeNode] {

  def this(dim: Int, threshold: Double) = this(new VectorDataPoint(new DenseTensor1(dim,0.0)), threshold, 0)

  def this(vdp: VectorDataPoint,threshold: Double) = {
    this(vdp.datapoint.length,threshold)
    absorb(vdp)
  }
}

trait VectorTreeNode[EntryType <: VectorTreeNodeEntry[EntryType,NodeType], NodeType <: VectorTreeNode[EntryType,NodeType]] extends BirchTreeNode[VectorDataPoint,EntryType,NodeType] {
  self : NodeType =>
  override def distance(one: VectorDataPoint, two: VectorDataPoint): Double = VectorDataPoint.l2Distance(one,two)

  override def toString = s"VectorTreeNode(${this.parent.map(_.toString).getOrElse("No parent")},${this.entries.map(_.toString).take(2).mkString("[",",","...]")})"
}


class BasicVectorTreeNode(override val threshold: Double, override val branchingFactor: Int) extends VectorTreeNode[BasicVectorTreeNodeEntry,BasicVectorTreeNode]


class VectorBirchClustering(val dim: Int, val threshold: Double, val branchingFactor: Int, reassign: Boolean) extends BirchClustering[VectorDataPoint,BasicVectorTreeNodeEntry,BasicVectorTreeNode]{
  override def singletonEntryConstructor: (VectorDataPoint) => BasicVectorTreeNodeEntry = (t: VectorDataPoint) => new BasicVectorTreeNodeEntry(t,threshold)

  override def emptyEntryConstructor: () => BasicVectorTreeNodeEntry = () => new BasicVectorTreeNodeEntry(dim,threshold)

  override def emptyNodeConstructor: () => BasicVectorTreeNode = () => new BasicVectorTreeNode(threshold,branchingFactor)

  override def getPointID(pt: VectorDataPoint): String = pt.id

  override def cluster(data: Iterable[VectorDataPoint]) = {
    super.cluster(data)
    // Re-assign means that we go back and find the center closest to each data point.
    if (!reassign) {
      val entries = root.entriesOfLeaves
      entries.foreach {
        entry =>
          val dp1 = entry.points.head
          entry.points.drop(1).foreach(dp1.cluster.addPoint)
      }
    } else {
      def dist(one: VectorDataPoint, two: VectorDataPoint) = (one.datapoint dot two.datapoint) * -2.0 + two.datapoint.sumSq()
      val entries = root.entriesOfLeaves
      val dataPointsInLeaves = entries.flatMap(_.points)
      val clustered = dataPointsInLeaves.map(d => (d, entries.minBy(f => dist(d, f.centroid)))).groupBy(_._2).mapValues(_.map(_._1))
      clustered.foreach {
        case (_, cluster) =>
          val dp1 = cluster.head
          cluster.drop(1).foreach(dp1.cluster.addPoint)
      }
    }
  }

}


object RunBIRCH {
  def main(args: Array[String]): Unit = {
    val fn = args(0)
    val threshold = args(1).toDouble
    val bf = args(2).toInt
    val vectors = LoadVectorDataPoint.loadFile(fn)
    val birch = new VectorBirchClustering(vectors.head.datapoint.dim1,threshold,bf,true)
    birch.cluster(vectors)
    val predStrings = vectors.map{
      v =>
        (v.id,v.cluster.id)
    }
    val goldStrings = vectors.map{
      v =>
        (v.id,v.goldClusterID.get)
    }
    val pred = new BasicEvaluatableClustering(predStrings)
    val gold = new BasicEvaluatableClustering(goldStrings)
    println(EvaluatableClustering.evaluationString(pred,gold))
  }
}
