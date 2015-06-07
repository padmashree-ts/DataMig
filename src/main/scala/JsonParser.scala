/*import net.liftweb.json._

object JsonParser {

  implicit val formats = DefaultFormats

  case class JsonBlob(account_id: String, action: String, asset_user_access_id: String)
  //case class JsonBlob(account_id: String)

  val source = """ {"account_id":"200000000000359","action":"grade_summary","asset_user_access_id":"200000019786462"} """

  val json = parse(source)

  def main(args: Array[String]) {
    val m = json.extract[JsonBlob]
    println(m.account_id)
    //println(m.action)
    //println(m.asset_user_access_id)
  }
}*/