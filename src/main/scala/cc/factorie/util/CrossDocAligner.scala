package cc.factorie.util

import cc.factorie._
import cc.factorie.app.nlp._
import cc.factorie.app.nlp.coref.{WithinDocEntity, WithinDocCoref}
import java.io.{File, BufferedReader, FileReader}
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
  def withinDocEntity:Option[WithinDocEntity] = span.document.getCoref.findOverlapping(span).map(_.entity)
}

object DefaultAligner {
  implicit val f:(Document, FloatingCrossDocEntity) => Option[TokenSpan] = { (doc, ent) =>
    ent.offsets.orElse{
      ent.canonicalValue.map { name =>
        val start = doc.string.replaceAll("""-\n""","-").replaceAll("""\n"""," ").indexOfSlice(name)
        val end = start + name.length - 1
        start -> end
      }
    }.flatMap {case (s,e) => doc.getSectionByOffsets(s,e).getOrElse(doc.asSection).offsetSnapToTokens(s,e)}
  }
}

/** Represents a Cross Doc Entity label that has not yet been tied to a token span in a factorie document. */
case class FloatingCrossDocEntity(trueLabel:String, canonicalValue:Option[String], entityType:Option[String], offsets:Option[(Int, Int)]) {

  def alignTo(doc:Document)(implicit aligner:((Document, FloatingCrossDocEntity) => Option[TokenSpan])):Option[CrossDocEntity] = aligner(doc, this).map { ts =>
    CrossDocEntityImpl(Some(trueLabel), canonicalValue, entityType, ts)
  }
}

class FloatingCrossDocAligner(docEntMap:Map[String, Iterable[FloatingCrossDocEntity]]) extends CrossDocAligner{
  def process(document:Document) = {
    import DefaultAligner._
    val xDocEnts = new CrossDocEntities
    docEntMap.get(document.name) match {
      case Some(floatingEnts) =>
        xDocEnts ++= floatingEnts.flatMap{ fEnt =>
          fEnt.alignTo(document) match {
            case Some(ent) => Some(ent)
            case None =>
              println("WARNING: Could not align entity %s in document %s".format(fEnt, document.name))
              None
          }
        }
      case None =>
        println("WARNING: Could not find cross doc ents for document: %s".format(document.name))
    }
    document.attr += xDocEnts
    document.annotators += classOf[CrossDocEntities] -> this.getClass
    document
  }
}

class TacCrossDocAligner(tabFile:String, xmlFile:String) extends FloatingCrossDocAligner({
  val entMap = new BufferedReader(new FileReader(tabFile)).toIterator.map { line =>
    val Array(mentId, entId, entType) = line.split("\\s+")
    mentId -> (entId, entType)
  }.toMap

  val docEntMap = NonValidatingXML.loadFile(xmlFile).\\("kbpentlink").\\("query").map { qXML =>
    val id = (qXML \ "@id").text.trim
    val name = (qXML \ "name").text.trim
    val docName = (qXML \ "docid").text.trim
    val beg = qXML \ "beg"
    val end = qXML \ "end"
    val offsets:Option[(Int, Int)] = if (beg.isEmpty || end.isEmpty) None else Some(beg.text.toInt, end.text.toInt)
    docName -> FloatingCrossDocEntity(entMap(id)._1, Some(name), entMap.get(id).map(_._2), offsets)
    //ReferenceMention(id, name, docName, offsets, entMap(id)._1, entMap(id)._2)
  }.groupBy(_._1).mapValues(_.map(_._2))
  docEntMap
})

// the John Smith Corpus is released as documents in different directories. Each
// directory contains all of the documents associated with a single entity.
class JohnSmithCrossDocAligner(jsmithDir:String) extends FloatingCrossDocAligner({
  new File(jsmithDir).listFiles().flatMap{ entDir =>
    val entId = "jsmith-" + entDir.getName
    entDir.listFiles.flatMap{ docFile =>
      val docName = docFile.getName
      val idx = new BufferedReader(new FileReader(docFile)).toIterator.mkString("\n").toLowerCase.indexOfSlice("john smith")
      if(idx == -1) {
        // todo something reasonable here
        None
      } else {
        Some(docName -> Seq(FloatingCrossDocEntity(entId, Some("John Smith"), Some("Person"), Some((idx, idx + 10)))))
      }
    }
  }.toMap
})


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


