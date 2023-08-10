package org.geneontology.golr

import org.apache.jena.query.{Dataset, QueryExecutionFactory, QuerySolution, ReadWrite}
import org.apache.jena.rdf.model.{Resource, ResourceFactory}
import org.apache.jena.tdb2.TDB2Factory
import org.apache.jena.vocabulary.{OWL2, RDFS, XSD}
import org.phenoscape.sparql.SPARQLInterpolation._
import sttp.client3._
import sttp.client3.httpclient.zio.HttpClientZioBackend
import sttp.client3.ziojson._
import sttp.model.Uri
import zio._
import zio.json._
import zio.json.internal.Write
import zio.stream._

import java.io.File
import scala.jdk.CollectionConverters._
import scala.util.Using

object Main extends ZIOAppDefault {

  val oio = "http://www.geneontology.org/formats/oboInOwl"
  val oioExactSynonym = ResourceFactory.createProperty(s"$oio#hasExactSynonym")
  val oioBroadSynonym = ResourceFactory.createProperty(s"$oio#hasBroadSynonym")
  val oioNarrowSynonym = ResourceFactory.createProperty(s"$oio#hasNarrowSynonym")
  val oioRelatedSynonym = ResourceFactory.createProperty(s"$oio#hasRelatedSynonym")
  val oioHasNamespace = ResourceFactory.createProperty(s"$oio#hasOBONamespace")
  val definition = ResourceFactory.createProperty("http://purl.obolibrary.org/obo/IAO_0000115")

  implicit val ontologyClassJsonEncoder: JsonEncoder[OntologyClassRecord] = DeriveJsonEncoder.gen[OntologyClassRecord]
  implicit val generalJsonEncoder: JsonEncoder[GeneralRecord] = DeriveJsonEncoder.gen[GeneralRecord]
  implicit val recordJsonEncoder: JsonEncoder[Record] = new JsonEncoder[Record] {
    override def unsafeEncode(a: Record, indent: Option[RuntimeFlags], out: Write): Unit = a match {
      case ocr: OntologyClassRecord => ontologyClassJsonEncoder.unsafeEncode(ocr, indent, out)
      case gr: GeneralRecord        => generalJsonEncoder.unsafeEncode(gr, indent, out)
    }
  }

  override def run =
    for {
      args <- getArgs
      solrLocation <- ZIO.attempt(args(0))
      solrEndpoint <- ZIO.fromEither(Uri.parse(s"$solrLocation/update/json?commit=true"))
      tdbLocation <- ZIO.attempt(args(1))
      dataset <- openTDBDataset(tdbLocation)
      httpClient <- HttpClientZioBackend.scoped()
      terms <- queryTerms(dataset)
      stream = ZStream.fromIterable(terms)
        .mapZIOParUnordered(20) { term =>
          indexTerm(term, dataset)
        }
        .collect { case Some(record) => ZStream.fromIterable(List(record._1, record._2)) }
        .flatten
        .grouped(1000)
        .map { records =>
          basicRequest.post(solrEndpoint)
            .body(records)
        }
        .map { req =>
          println(req)
          req
        }
        .mapZIOParUnordered(4) { request =>
          request.send(httpClient)
        }
      _ <- stream.runDrain
    } yield ()

  def openTDBDataset(path: String): Task[Dataset] =
    ZIO.attempt(new File(path).isDirectory).flatMap { isDir =>
      if (isDir) ZIO.attempt(TDB2Factory.connectDataset(path))
      else ZIO.fail(new Exception("TDB path is not a directory"))
    }

  // Jena transaction management does not work across ZIO fibers/threads, so this method
  // sets up transaction and converts to final values all at once
  def runSelect[A](query: QueryText, dataset: Dataset)(getValue: QuerySolution => A): Task[List[A]] =
    ZIO.attempt(query.toQuery).flatMap { q =>
      ZIO.attempt {
        dataset.begin(ReadWrite.READ)
        try Using.resource(QueryExecutionFactory.create(q, dataset)) { qe =>
          qe.execSelect().asScala.map(getValue).toList
        }
        finally dataset.end()
      }
    }

  def queryTerms(dataset: Dataset): Task[List[Resource]] = {
    val query =
      sparql"""
        SELECT ?term
        WHERE {
          ?term a ${OWL2.Class} .
          FILTER(isIRI(?term))
        }
        """
    runSelect(query, dataset)(_.getResource("term"))
  }

  def indexTerm(term: Resource, dataset: Dataset): Task[Option[(GeneralRecord, OntologyClassRecord)]] =
    for {
      maybeProps <- getProperties(term, dataset)
      record <- ZIO.foreach(maybeProps) { props =>
        for {
          synonyms <- getSynonyms(term, dataset)
          superclasses <- getSuperclasses(term, dataset)
        } yield {
          val curie = compact(term.getURI)
          (GeneralRecord.create(curie, props, synonyms),
            OntologyClassRecord.create(curie, props, synonyms, superclasses))
        }
      }
    } yield record

  def getProperties(term: Resource, dataset: Dataset): Task[Option[Term]] = {
    val query =
      sparql"""
        SELECT DISTINCT ?label ?def ?namespace ?obsolete
        WHERE {
          $term ${RDFS.label} ?label .
          OPTIONAL {
            $term $definition ?def .
          }
          OPTIONAL {
            $term $oioHasNamespace ?namespace .
          }
          OPTIONAL {
            $term ${OWL2.deprecated} ?deprecated .
            FILTER(datatype(?deprecated) = ${XSD.xboolean})
          }
          BIND(COALESCE(?deprecated, false) AS ?obsolete)
        }
        """
    runSelect(query, dataset) { qs =>
      Term(
        iri = term.getURI,
        label = qs.getLiteral("label").getLexicalForm,
        definition = Option(qs.getLiteral("def")).map(_.getLexicalForm),
        namespace = Option(qs.getLiteral("namespace")).map(_.getLexicalForm),
        obsolete = qs.getLiteral("obsolete").getBoolean
      )
    }.map(_.headOption)
  }

  def getSynonyms(term: Resource, dataset: Dataset): Task[List[String]] = {
    val query =
      sparql"""
        SELECT DISTINCT ?synonym
        WHERE {
          $term $oioExactSynonym|$oioBroadSynonym|$oioNarrowSynonym|$oioRelatedSynonym ?synonym .
        }
        """
    runSelect(query, dataset)(_.getLiteral("synonym").getLexicalForm)
  }

  def getSuperclasses(term: Resource, dataset: Dataset): Task[List[Labeled]] = {
    val query =
      sparql"""
        SELECT ?superclass (MIN(?rdfsLabel) AS ?label)
        WHERE {
          $term ${RDFS.subClassOf} ?superclass .
          ?superclass ${RDFS.label} ?rdfsLabel .
          FILTER(isIRI(?superclass))
        }
        GROUP BY ?superclass
        """
    runSelect(query, dataset) { qs =>
      Labeled(
        qs.getResource("superclass").getURI,
        qs.getLiteral("label").getLexicalForm)
    }
  }

  final case class Labeled(iri: String, label: String)

  final case class Term(iri: String, label: String, definition: Option[String], namespace: Option[String], obsolete: Boolean)

  sealed trait Record extends Product with Serializable

  final case class OntologyClassRecord(
                                        document_category: String,
                                        id: String,
                                        annotation_class: String,
                                        annotation_class_label: String,
                                        description: Option[String],
                                        source: Option[String],
                                        idspace: String,
                                        is_obsolete: Boolean,
                                        synonym: List[String],
                                        isa_closure: List[String],
                                        isa_closure_label: List[String]
                                      ) extends Record

  object OntologyClassRecord {

    def create(curie: String, term: Term, synonyms: List[String], superclasses: List[Labeled]): OntologyClassRecord = {
      OntologyClassRecord(
        document_category = "ontology_class",
        id = curie,
        annotation_class = curie,
        annotation_class_label = term.label,
        description = term.definition,
        source = term.namespace,
        idspace = curie.split(":", 2).head,
        is_obsolete = term.obsolete,
        synonym = synonyms,
        isa_closure = superclasses.map(sup => compact(sup.iri)),
        isa_closure_label = superclasses.map(sup => compact(sup.label))
      )
    }

  }

  final case class GeneralRecord(
                                  document_category: String,
                                  id: String,
                                  entity: String,
                                  entity_label: String,
                                  category: String,
                                  general_blob: String,
                                ) extends Record

  object GeneralRecord {

    def create(curie: String, term: Term, synonyms: List[String]): GeneralRecord = {
      GeneralRecord(
        document_category = "general",
        id = s"general_ontology_class_$curie", // in the official golr-loader, various categories are used here
        entity = curie,
        entity_label = term.label,
        category = "ontology_class", // in the official golr-loader, various categories are used here
        general_blob = s"$curie ${term.definition.getOrElse("")} ${synonyms.mkString(" ")}" // not exactly the same as the official golr-loader
      )
    }

  }

  //FIXME
  def compact(iri: String): String = {
    val obo = "http://purl.obolibrary.org/obo/"
    val mesh = "http://id.nlm.nih.gov/mesh/"
    if (iri.startsWith(obo)) {
      iri.replace(obo, "").replaceFirst("_", ":")
    } else if (iri.startsWith(mesh)) {
      iri.replace(mesh, "MeSH:")
    } else iri
  }

}
