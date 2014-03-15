import idx.{Feed2Solr, SolrId}
import java.util
import scala.collection.mutable.ArrayBuffer
import scala.xml._
object Main
{
  def main(args: Array[String])
  {
    var n = 1;
    val lst: ArrayBuffer[SolrId] = ArrayBuffer[SolrId]();

    var Posts = scala.io.Source.fromFile("D:\\stackoverflow\\Posts.xml");
    for (post <- Posts.getLines())
    {
      if (post.startsWith("  <row"))
      {

        if (n > 0)
        {
          val row = XML.loadString(post);
          val SolrDoc = new SolrId((row \ "@Id").toString(),
            (row \ "@PostTypeId").toString(),
            (row \ "@ParentId").toString(),
            (row \ "@AcceptedAnswerId").toString(),
            (row \ "@CreationDate").toString(),
            (row \ "@Score").toString(),
            (row \ "@Body").toString(),
            (row \ "@OwnerUserId").toString(),
            (row \ "@LastActivityDate").toString(),
            (row \ "@Title").toString(),
            (row \ "@Tags").toString(),
            (row \ "@AnswerCount").toString())

          //println(SolrDoc.toString)lst
          lst += SolrDoc;
          println (lst.size)
        }

        n = n + 1
      }

      if ((lst.size + 1) % 5000 == 0)
      {
        (new Feed2Solr).send(lst)
        lst.clear();

      }
    }

    println(n)
  }
}