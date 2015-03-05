package cc.factorie.app.nlp.hcoref

import cc.factorie.app.nlp.Document
import cc.factorie.util.{CrossDocEntity, CrossDocEntities}
import cc.factorie.variable.CategoricalDomain

/**
 * @author John Sullivan
 */
// We want to be able to generate XDocMentions from Documents, ?store them? and
// run coref on the resulting mentions
trait XDocCorefSystem[T] {
  def generateEnts(doc:Document):Iterable[(String, Option[String], T)]
  def getEntId(ent:T):String
  def doCoref(ments:Seq[T]):Seq[(T, Int)]

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

  def generateEnts(doc: Document) = doc.attr[CrossDocEntities].map { docEnt =>
    val ment = new Mention[Vars](getVars(docEnt))(null)
    docEnt.withinDocEntity foreach (we => ment.withinDocEntityId = we.uniqueId)
    (ment.withinDocEntityId, docEnt.trueLabel, ment)
  }
}
