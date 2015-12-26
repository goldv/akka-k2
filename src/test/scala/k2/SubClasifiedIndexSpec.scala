package k2

import akka.util.Subclassification
import k2.util.SubclassifiedIndex
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

class SubClassifiedIndexSpec extends WordSpecLike with Matchers with BeforeAndAfterAll{

  class StartsWithSubclassification extends Subclassification[String] {
    override def isEqual(x: String, y: String): Boolean =
      x == y

    override def isSubclass(x: String, y: String): Boolean =
      x.startsWith(y)
  }

  implicit val subclassification: Subclassification[String] = new StartsWithSubclassification()

  "A sub classified index" must{
    "add base classifier" in {

      val subs = new SubclassifiedIndex[String, String]()

      assert(subs.addValue("marketdata", "subscriber1") == Vector(("marketdata" -> Set("subscriber1"))) )
      assert( subs.addValue("marketdata/eur.usd", "subscriber2") == Vector(("marketdata/eur.usd",Set("subscriber1", "subscriber2"))))
      assert( subs.addValue("marketdata/eur.chf", "subscriber2") == Vector(("marketdata/eur.chf",Set("subscriber1", "subscriber2"))))
      assert( subs.addValue("marketdata/eur.chf", "subscriber3") == Vector(("marketdata/eur.chf",Set("subscriber3"))))

    }
  }

}
