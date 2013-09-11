import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers._
import com.amazonaws.services.dynamodb.model._
import data.dynamo._

class DynastySpec extends FlatSpec {
  
  "Basic parsers" should "work" in {
    val data = Map[String,AttributeValue]()

    // parse set
    (tables.UserItem.userId parse data) should be (None)
    (tables.UserItem.workspaces parse data) should be (Some(Set.empty))
  }
  
}

