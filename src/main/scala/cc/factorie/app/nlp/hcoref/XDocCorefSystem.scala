package cc.factorie.app.nlp.hcoref

import cc.factorie.app.nlp.Document
import cc.factorie.util.{BasicEvaluatableClustering, EvaluatableClustering}
/**
 * @author John Sullivan
 */
// We want to be able to generate XDocMentions from Documents, ?store them? and
// run coref on the resulting mentions
trait XDocCorefSystem[T] {
  def generateXDocMentions(doc:Document):Iterable[XDocMention[T]]

  def performCoref(mention:Iterator[XDocMention[T]]):Iterable[XDocMention[T]]
}

case class CorefHypothesis(mentionId:String, entityId:String)

object CorefHypothesis {
  def toEvaluatableClustering(hyps:Iterable[CorefHypothesis]) =
    new BasicEvaluatableClustering(hyps.map{case CorefHypothesis(m,e) => m -> e})

  def fromEvaluatableClustering(cluster:EvaluatableClustering[String, String]) =
    cluster.pointIds.map {m => CorefHypothesis(m, cluster.clusterId(m))}
}

// we need a proper subclass of this for general use from generateXDocMentions
// also maybe something that can take an uncorefed XDocMention, extract the T
// coref it and return a corefed one (for XDocCorefSystem) we may also want
// some generic way to serialize/deserialize? Maybe when we have some serializer
// T => Cubbie.
trait XDocMention[T] {
  // identifies the document from which this mention came
  def docPointer:String

  // some representation of the entity used to do XDoc coref
  def entity:T

  // unique string for this mention
  def id:String

  // predicted entity id
  def predictedEnt:Int

  // true entity id
  def trueEnt:Option[Int]
}

class HierCorefSystem extends XDocCorefSystem[Node[DocEntityVars]] {
  def generateXDocMentions(docs: Iterator[Document]) = docs.flatMap { doc =>
    doc.coref.entities.map { wEnt =>
      val ment = new Mention[DocEntityVars](DocEntityVars.fromWithinDocEntity(wEnt))
      val docName = new String(doc.name)
      new XDocMention[Node[DocEntityVars]] {
        val id = ment.uniqueId
        val docPointer = docName
        val entity = ment
      }
    }
  }
}

