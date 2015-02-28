package cc.factorie.util

import cc.factorie._
import cc.factorie.app.nlp._
import cc.factorie.app.nlp.coref.WithinDocCoref
import java.io.{BufferedReader, FileReader}
import scala.collection.mutable

// Often for Cross-doc coref labelled data we have the documents labelled with
// the cross-doc entities rather than actual text spans. This code exists to
// create properly labeled spans in documents for training and evaluation. It
// should work with John Smith, NYT, and TAC EL corpora, which all have this
// structure, among others.


// This should probably actually be a doc annotator that marks the spans with
// some custom alignment that is provided in the constructor. This should be
// able to pull in the w/in doc coref chains that get aligned to these mentions
// if applicable. They should also be able to store entity Type domains. The
// document's id/name will have to correspond to some identifier in the labelled
// set to align documents with their mentions.
trait CrossDocAligner extends DocumentAnnotator {

  def tokenAnnotationString(token: Token) = {
    val ents = token.document.attr[CrossDocEntities]
    ents.collectFirst{
      case e if e.span.value.contains(token) => e.trueLabel // todo check that this is the right equality
    }.flatten.getOrElse("")
  }

  val prereqAttrs:Iterable[Class[_]] = Seq(classOf[Token])
  val postAttrs:Iterable[Class[_]] = Seq(classOf[CrossDocEntities])

}

class CrossDocEntities extends mutable.ArrayBuffer[CrossDocEntity]

case class CrossDocEntityImpl(trueLabel:Option[String], canonicalValue:Option[String], entType:Option[String], span:TokenSpan) extends CrossDocEntity
trait CrossDocEntity {
  def trueLabel:Option[String]
  def canonicalValue:Option[String]
  def entType:Option[String]
  def span:TokenSpan
}

class Tac2009CrossDocAligner(tabFile:String, xmlFile:String) extends CrossDocAligner {
  private val refMap = ReferenceMention.fromQueryFiles(tabFile, xmlFile).groupBy(_.docId)
  override val prereqAttrs = Seq(classOf[WithinDocCoref])

  def process(document: Document) = {
    val xDocEnts = new CrossDocEntities
    refMap.get(document.name) match {
      case Some(ents) =>
        ents.foreach { ent =>
          ent.doc = Some(document)
          ent.getTokenSpan match {
            case Some(span) => xDocEnts += CrossDocEntityImpl(Some(ent.entId), Some(ent.name), Some(ent.entType), span)
            case None =>
              // todo do something reasonable
          }
        }
      case None =>
        // todo do something reasonable
    }
    document.attr += xDocEnts
    document.annotators += classOf[CrossDocEntities] -> this.getClass
    document
  }
}


object ReferenceMention{
  def fromQueryFiles(queryXMLFile:String, queryTabFile:String):Iterable[ReferenceMention] = {
    val entMap = new BufferedReader(new FileReader(queryTabFile)).toIterator.map { line =>
      val Array(mentId, entId, entType) = line.split("\\s+")
      mentId -> (entId, entType)
    }.toMap

    NonValidatingXML.loadFile(queryXMLFile).\\("kbpentlink").\\("query").map { qXML =>
      val id = (qXML \ "@id").text.trim
      val name = (qXML \ "name").text.trim
      val docName = (qXML \ "docid").text.trim
      val beg = qXML \ "beg"
      val end = qXML \ "end"
      assert(beg.isEmpty == end.isEmpty)
      val offsets:Option[(Int, Int)] = if (beg.isEmpty || end.isEmpty) None else Some(beg.text.toInt, end.text.toInt)
      ReferenceMention(id, name, docName, offsets, entMap(id)._1, entMap(id)._2)
    }
  }
}

case class ReferenceMention(id:String, name:String, docId:String, offsets:Option[(Int, Int)], entId:String, entType:String) {
  var doc:Option[Document] = None
  lazy val getOffsets:(Int, Int) = offsets.getOrElse {
    val start = doc.get.string.replaceAll("""-\n""","-").replaceAll("""\n"""," ").indexOfSlice(name)
    val end = start + name.length - 1
    start -> end
  }
  def getTokenSpan = doc.get.getSectionByOffsets(this.getOffsets._1, this.getOffsets._2).getOrElse(doc.get.asSection).offsetSnapToTokens(this.getOffsets._1, this.getOffsets._2)
}

/**
 * A Document Annotator that takes WithinDocCoref Entities and upgrades them to
 * cross-doc Coref Mentions
 */
class CrossDocUpgrader extends CrossDocAligner {
  override val prereqAttrs = Seq(classOf[WithinDocCoref])

  def process(document: Document) = {
    val xDocEnts = new CrossDocEntities
    document.coref.entities foreach { ent =>
      xDocEnts += CrossDocEntityImpl(None, Option(ent.canonicalName), ent.canonicalMention.getType, ent.getCanonicalMention.phrase)
    }
    document.attr += xDocEnts
    document.annotators += classOf[CrossDocEntities] -> this.getClass
    document
  }

}

// we need to take a document and a set of contained entities, which must have at
// least names but may also have types. There may be several per document. The
// true entity id, type, and canonical value should all be storable in the cross doc
// entity representation, along with a token span that it represents in the document.
// once both the CrossDocEntities and WithinDocEntities (or mentions) are present in
// document, we should be able to get the full coreference chain for each entity.
// There should be an annotator that takes documents with WithinDocCoref and creates
// CrossDocEntities without true labels by promoting within doc entities.

// Cross Doc Coref Systems should, in general, take a Document annotated with
// CrossDocEntities and generate one mention in their system for each CrossDocEntity


