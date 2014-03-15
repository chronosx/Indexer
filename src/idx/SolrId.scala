package idx

import org.apache.solr.common.SolrInputDocument
import java.util.ArrayList
import org.apache.solr.client.solrj.impl.HttpSolrServer
import org.apache.solr.morphlines.solr.SolrMorphlineContext
import scala.collection.mutable.ArrayBuffer

case class SolrId(Id:String,PostTypeId:String,ParentId:String, AcceptedAnswerId:String,CreationDate:String,Score:String, Body:String,
                  OwnerUserId:String, LastActivityDate:String, Title:String, Tags:String, AnswerCount:String)
class Feed2Solr
{
  val url = "http://23.251.143.120:8983/solr/"
  val solrDocuments = new ArrayList[SolrInputDocument]()
  val server= new HttpSolrServer( url )

  def send(books:ArrayBuffer[SolrId])
  {
    try
    {
      val docs: ArrayList[SolrInputDocument] = new ArrayList[SolrInputDocument]();
      books.foreach(book=>docs.add(getSolrDocument(book)))
      server.add(docs);
      server.commit()
    }
    catch
    {
      case e: Exception =>
      //books.foreach(book=>server.add(getSolrDocument(book)))
      val docs: ArrayList[SolrInputDocument] = new ArrayList[SolrInputDocument]();
        books.foreach(book=>docs.add(getSolrDocument(book)))
      server.add(docs);
      server.commit();
    }

  }

  def getSolrDocument(book: SolrId): SolrInputDocument =
  {
    val document = new SolrInputDocument()

    document.addField("Id",book.Id);
    document.addField("PostTypeId",book.PostTypeId);
    document.addField("ParentId",book.ParentId);
    document.addField("AcceptedAnswerId",book.AcceptedAnswerId);
    document.addField("CreationDate",book.CreationDate + "Z");
    document.addField("Score",book.Score);
    document.addField("Body",book.Body);
    document.addField("OwnerUserId",book.OwnerUserId);
    document.addField("LastActivityDate",book.LastActivityDate + "Z");
    document.addField("Title",book.Title);
    document.addField("Tags",book.Tags);
    document.addField("AnswerCount",book.AnswerCount);

    document
  }
}
