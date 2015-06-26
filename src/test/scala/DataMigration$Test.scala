package com.instructure

import net.liftweb.json._
import org.instructure.sparktestutil.SparkTestUtils
import org.scalatest.{BeforeAndAfter, ShouldMatchers}
/**
 * Created by pteeka on 6/8/15.
 */
class DataMigration$Test extends SparkTestUtils with ShouldMatchers with BeforeAndAfter {

  sparkTest("test test") {
    DataMigration.run(sc, "/Users/pteeka/IdeaProjects/DataMig/src/main/resources/lessDataMigration.txt", "/Users/pteeka/IdeaProjects/DataMig/target/lessDataMigration")
  }

  sparkTest("Test for grouping by RequestId") {
    val textFile = sc.textFile("/Users/pteeka/IdeaProjects/DataMig/src/main/resources/lessDataMigration.txt")
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
    val textFile = sc.textFile("src/main/resources/lessDataMigration.txt")
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

  sparkTest("Test for requests without updated_at") {
    val string1 = """{"name":"EMILY","user_id":"200000000753855","controller":"gradebooks","user_request":"true"}"""
    val string2 = """{"summarized":"true","user_id":"200000000753855","user_request":"false","updated_at":"2015-05-10 17:03:44+0000"}"""
    val multipleRequests = List(string1, string2).toIterable
    try {
      val mergedOutput = DataMigration.merging(multipleRequests)
      fail()
    }
    catch {
      case _: NoSuchElementException =>
    }
  }

  /*sparkTest("Test for Index out of bounds") {
    val s = "hi"
    try {
      s.charAt(-1)
      fail()
    }
    catch {
      case _: IndexOutOfBoundsException => // Expected, so continue
    }
  }*/

  /*sparkTest("Test for requests without created_at") {
    val string1 = """{"name":"EMILY","user_id":"200000000753855","controller":"gradebooks","user_request":"true"}"""
    val string2 = """{"summarized":"true","user_id":"200000000753855","user_request":"false","updated_at":"2015-05-10 17:03:44+0000"}"""
    val multipleRequests = List(string1, string2).toIterable
    val keyValuePair = RDD("5625fa4e-d7ae-4d15-9fe0-e7e12b4ca7e1",multipleRequests)
  }*/
}

  /*sparkTest("Test for Merging Logic") {
    DataMigration.run(sc,"src/test/resources/mergingTestFile.txt", "src/test/resources/mergingTestOutput")
    val outputFiles = new java.io.File("src/test/resources/mergingTestOutput").listFiles.filter(_.getName.startsWith("part"))
    val linesInOutputFiles = outputFiles.map{
      file => Source.fromFile(file).getLines().mkString
    }
    val keyRequestTuple=linesInOutputFiles.map{ line =>
      val index = line.indexOf(" ")
      val key = line.substring(0,index)
      val request = line.substring(index)
      val requestMap = parse(request).values.asInstanceOf[Map[String,String]]
      (key,requestMap)
    }
    keyRequestTuple.foreach { case (key, requestMap) =>
      if(key.contains("5625fa4e-d7ae-4d15-9fe0-e7e12b4ca7e1")) {
        assert(requestMap("name")=="EMILY" && requestMap("user_request")=="false" && requestMap.contains("summarized") )
      }
    }
  }*/
