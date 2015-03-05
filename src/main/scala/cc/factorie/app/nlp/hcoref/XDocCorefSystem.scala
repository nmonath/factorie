package cc.factorie.app.nlp.hcoref

import cc.factorie.app.nlp.{CompoundDocumentAnnotator, Document}
import cc.factorie._
import cc.factorie.util._
import cc.factorie.variable.CategoricalDomain
import cc.factorie.app.nlp.segment.{DeterministicTokenizer, DeterministicSentenceSegmenter}
import java.io.{BufferedReader, FileReader, File}

/**
 * @author John Sullivan
 */
// We want to be able to generate XDocMentions from Documents, ?store them? and
// run coref on the resulting mentions
trait XDocCorefSystem[T] {
  protected def generateEnts(doc:Document):Iterable[(String, Option[String], T)]
  protected def getEntId(ent:T):String
  protected def doCoref(ments:Seq[T]):Seq[(T, Int)]

  private val trueEntDomain = new CategoricalDomain[String]()

  def generateXDocMentions(doc:Document):Iterable[XDocMention[T]] = generateEnts(doc) map { case (withinDocId, trueEnt, ent) =>
    val xMent = XDocMentionImpl(getEntId(ent), doc.name, withinDocId, ent)
    trueEnt foreach trueEntDomain.+=
    xMent.trueEnt = trueEnt.map(trueEntDomain.getIndex)
    xMent
  }

  // TODO this is not well implemented - it drops non XDocMentionImpls on the ground
  def performCoref(mentions:Iterator[XDocMention[T]]):Iterable[XDocMention[T]] = {
    val pairs = mentions.toSeq.collect { case xMent @ XDocMentionImpl(_, _, _, entity) =>
      xMent -> entity
    }
    doCoref(pairs.map(_._2)).zip(pairs.map(_._1)).map { case ((ent, entId), oXDoc @ XDocMentionImpl(id, docPointer, withinDocEntity, _)) =>
      val xDoc = XDocMentionImpl(id, docPointer, withinDocEntity, ent)
      xDoc.predictedEnt = entId
      xDoc.trueEnt = oXDoc.trueEnt
      xDoc
    }
  }
}

// we need a proper subclass of this for general use from generateXDocMentions
// also maybe something that can take an uncorefed XDocMention, extract the T
// coref it and return a corefed one (for XDocCorefSystem) we may also want
// some generic way to serialize/deserialize? Maybe when we have some serializer
// T => Cubbie.
trait XDocMention[T] {
  // identifies the document from which this mention came
  def docPointer:String

  // identifies the entity id in the document from which it came
  def withinDocEntityId:String

  // some representation of the entity used to do XDoc coref
  def entity:T

  // unique string for this mention
  def id:String

  // predicted entity id
  def predictedEnt:Int

  // true entity id
  def trueEnt:Option[Int]
}

object XDocMention {
  def predictedClustering(ments:Iterable[XDocMention[_]]) =
    new BasicEvaluatableClustering(ments.map(m => m.id -> m.predictedEnt.toString))

  def trueClustering(ments:Iterable[XDocMention[_]]) = if(ments.forall(_.trueEnt.isDefined)) {
    new BasicEvaluatableClustering(ments.map(m => m.id -> m.trueEnt.get.toString))
  } else {
    throw new Exception("All mentions must have a true label defined")
  }
}

case class XDocMentionImpl[T](id:String, docPointer:String, withinDocEntityId:String, entity:T) extends XDocMention[T] {
  var predictedEnt = null.asInstanceOf[Int]
  var trueEnt:Option[Int] = None
}

class HierCorefSystem[Vars <: NodeVariables[Vars]](getVars:CrossDocEntity => Vars, getSampler:Seq[Node[Vars]] => CorefSampler[Vars]) extends XDocCorefSystem[Node[Vars]] {
  def doCoref(ments: Seq[Node[Vars]]) = {
    val sampler = getSampler(ments)
    sampler.infer()
    val entDomain = new CategoricalDomain[String]
    ments.map { ment =>
      entDomain += ment.root.uniqueId
      ment -> entDomain.getIndex(ment.root.uniqueId)
    }
  }

  def getEntId(ent: Node[Vars]) = ent.uniqueId

  def generateEnts(doc: Document) =
    doc.attr[CrossDocEntities].map { docEnt =>
    val ment = new Mention[Vars](getVars(docEnt))(null)
    docEnt.withinDocEntity foreach (we => ment.withinDocEntityId = we.uniqueId)
    (ment.withinDocEntityId, docEnt.trueLabel, ment)
  }.toIterable
}

object JohnSmithExample {

  val getVars = { xEnt:CrossDocEntity =>
    val vars = new DocEntityVars()
    vars
  }

  val getSampler = { ments:Seq[Node[DocEntityVars]] =>
    val model = new DocEntityCorefModel(4.0, -0.25, 1.0, 2.0, -0.25, 1.0, -0.25, 1.0, -0.25, 1.0, -0.25)
    implicit val r = new scala.util.Random()
    new CorefSampler[DocEntityVars](model, ments, ments.size * 100)
      with AutoStoppingSampler[DocEntityVars]
      with CanopyPairGenerator[DocEntityVars]
      with NoSplitMoveGenerator[DocEntityVars] {

      val autoStopThreshold = 10000
      def newInstance(implicit d:DiffList) = new Node[DocEntityVars](new DocEntityVars)
    }
  }

  def main(args:Array[String]) {
    import FileUtils._
    val jSmithDir = args(0)
    val pipeline = new CompoundDocumentAnnotator(Seq(DeterministicTokenizer, DeterministicSentenceSegmenter, new JohnSmithCrossDocAligner(jSmithDir)))
    val docs =  getRecursiveListOfFiles(jSmithDir).map { f =>
      new Document(new BufferedReader(new FileReader(f)).toIterator.mkString("\n")).setName(f.getName)
    }
    docs foreach pipeline.process

    val system = new HierCorefSystem[DocEntityVars](getVars, getSampler)

    val xDocMentions = docs.flatMap(system.generateXDocMentions)

    val results = system.performCoref(xDocMentions.iterator)

    EvaluatableClustering.evaluationString(XDocMention.predictedClustering(results), XDocMention.trueClustering(results))
  }
}
