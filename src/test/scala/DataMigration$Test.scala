package com.instructure

import net.liftweb.json._
import org.instructure.sparktestutil.SparkTestUtils
import org.scalatest.{BeforeAndAfter, ShouldMatchers}
/**
 * Created by pteeka on 6/8/15.
 */
class DataMigration$Test extends SparkTestUtils with ShouldMatchers with BeforeAndAfter {

  sparkTest("test test") {
    DataMigration.run(sc, "src/test/resources/lessDataMigration.txt", "target/testOutput")
  }

  sparkTest("Test for grouping by RequestId") {
    val textFile = sc.textFile("src/test/resources/lessDataMigration.txt")
    val requestIdValuePairs = DataMigration.groupByRequestId(textFile)
    assert(requestIdValuePairs.count() == 5)
  }

  sparkTest("Test for merging logic") {
    val string1 = """{"name":"EMILY","user_id":"200000000753855","controller":"gradebooks","created_at":"2015-05-30 17:03:44+0000","user_request":"true","updated_at":"2015-05-20 17:03:44+0000"}"""
    val string2 = """{"summarized":"true","user_id":"200000000753855","created_at":"2015-05-20 17:03:44+0000","user_request":"false","updated_at":"2015-05-30 17:03:44+0000"}"""
    val strings = List(string1, string2)
    val mergedOutput = DataMigration.merging(strings.toIterable)
    assert(mergedOutput("name") == "EMILY" && mergedOutput("user_request") == "false" && mergedOutput.contains("summarized"))
  }

  sparkTest("Testing string to map conversion method") {
    val m = DataMigration.parseStringToMapLift( """{"name":"Jack","age":"25"}""")
    assert(m("name") == "Jack")
    assert(m("age") == "25")
  }

  sparkTest("Testing map to String conversion method") {
    val m = Map("name" -> "Jack", "age" -> "25")
    val s = DataMigration.mergedOutputToString(m)
    assert(s == """{"name":"Jack","age":"25"}""")
  }

  sparkTest("RequestId with multiple requests all have the same user_id") {
    val textFile = sc.textFile("src/test/resources/lessDataMigration.txt")
    val groups = DataMigration.groupByRequestId(textFile)
    val setOfUserIds = groups.map { case (requestId, requests) =>
      val user_ids = requests.map { request =>
        val requestMap = parse(request).values.asInstanceOf[Map[String, String]]
        //requestMap.contains("user_id") should be (true)
        if (requestMap.contains("user_id")) {
          val user_id = requestMap("user_id")
          user_id
        }
      }
      user_ids.toSet
    }.collect()
    assert(groups.count() == setOfUserIds.size)
  }

  sparkTest("Test for output pattern") {
    val m = Map("summarized" -> "true", "user_id" -> "200000000753855", "created_at" -> "2015-05-20 17:03:44+0000")
    val s = "76d84a76-ad29-4490-b7a8-bcfe9ee41e5c"
    val outputPattern = DataMigration.writeToFilePattern(m,s)
    assert(outputPattern == """200000000753855|2015-05-20T11:03:44.000-06:00|76d84a76-ad29-4490-b7a8-bcfe9ee41e5c""")
  }

}