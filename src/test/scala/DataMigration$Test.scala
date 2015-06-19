import com.instructure.DataMigration
import org.apache.spark.{SparkContext, SparkConf}
import org.instructure.sparktestutil.SparkTestUtils
import org.scalatest.{ShouldMatchers, BeforeAndAfter, FunSuite}
import scala.io.Source
import play.api.libs.json.{Json, _}

/**
 * Created by pteeka on 6/8/15.
 */
class DataMigration$Test extends SparkTestUtils with ShouldMatchers with BeforeAndAfter {

  sparkTest("test test") {
    DataMigration.run(sc, "/Users/pteeka/IdeaProjects/DataMig/src/main/resources/lessDataMigration.txt", "/Users/pteeka/IdeaProjects/DataMig/target/lessDataMigration")
  }

  sparkTest("grouping by RequestId") {
    val textFile = sc.textFile("/Users/pteeka/IdeaProjects/DataMig/src/main/resources/lessDataMigration.txt")
    val requestIdValuePairs = DataMigration.groupByRequestId(textFile)
    assert(requestIdValuePairs.count() == 5)
  }

  sparkTest("test for merging logic") {
    DataMigration.run(sc, "src/test/resources/mergingTestFile.txt", "src/test/resources/mergingTestOutput")
    val outputFiles = new java.io.File("src/test/resources/mergingTestOutput").listFiles.filter(_.getName.startsWith("part"))
    var lines = List("user_id")
    val allLines = outputFiles.map {
      file => lines = lines ++ Source.fromFile(file).getLines.toList
        lines
    }
    allLines.foreach { line => assert(line.contains("user_id")) }
    allLines.foreach { line =>
    if(line.contains("5625fa4e-d7ae-4d15-9fe0-e7e12b4ca7e1"))
    {
      assert(line.contains("EMILY") && line.contains("\"user_request\":\"true\"") && line.contains("summarized") )
    }
    }
  }

  /*sparkTest("RequestId with multiple requests all have the same user id") {
    val textFile=sc.textFile("src/main/resources/lessDataMigration.txt")
    val requestIdValuePairs = DataMigration.groupByRequestId(textFile)
    requestIdValuePairs.foreach{ case (requestId, requests) =>
      if(requests.size > 1) {
        val requestMaps = requests.map { request=> Json.parse(request).as[JsObject].fields.map { case (k,v ) => (k, v.as[String])}.toMap }
        val standard_user_id=requestMaps.last("user_id")
        println("Outside If" + standard_user_id)
        for (currentMap <- requestMaps) {
          val user_id = currentMap("user_id")
          println("Inside If"+user_id)
          assert(user_id==standard_user_id)
        }
      }
    }
  }*/
}