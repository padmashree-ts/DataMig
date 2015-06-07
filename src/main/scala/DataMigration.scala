import netscape.javascript.JSObject

import scala.io.Source
import org.apache.spark.SparkContext
import org.apache.spark._
import java.io._
import org.apache.spark.util.collection._
import org.joda.time._
import play.api.libs.json.Json
import play.api.libs.json._
import org.joda.time.format.DateTimeFormat


object DataMigration {
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("DataMigration").setMaster("local").set("spark.hadoop.validateOutputSpecs", "false"))
    val textFile = sc.textFile("/Users/pteeka/IdeaProjects/DataMig/src/main/resources/lessDataMigration.txt")
    val requestByIdValue = textFile.map(textFile => {
      val index = textFile.indexOf(" ")
      val requestId = textFile.substring(0, index)
      val json = textFile.substring(index)
      (requestId, json)
    })
    val keyValuePairs = requestByIdValue.groupByKey()
    keyValuePairs.map { case (requestId, requests) =>
      if (requests.size > 1) {
        requestId + " " + merging(requests)
      }
      else {
        requestId + " " + requests
      }
    }.saveAsTextFile("/Users/pteeka/IdeaProjects/DataMig/target/lessDataMigration1")

    def parseJsonToMap(s: String): Map[String, String] = {
      Json.parse(s).as[JsObject].fields.map { case (k, v) => (k, v.as[String]) }.toMap
    }

    def merging(requests: Iterable[String]): Map[String,String] = {
      val requestMaps = requests.map(request => parseJsonToMap(request))

      var datePattern = "yyyy-MM-dd HH:mm:ssZ"
      var mergedOutput = Map(("created_at", "2000-05-20 15:53:25+0000"))

      for (currentMap <- requestMaps) {
        var currentDate = DateTime.parse(currentMap("created_at").toString(), DateTimeFormat.forPattern(datePattern))
        var mergedDate = DateTime.parse(mergedOutput("created_at").toString(), DateTimeFormat.forPattern(datePattern))

        if( currentDate.isAfter(mergedDate) )
        {
          var keysOnlyInPrevMergedOutput = mergedOutput.keys.toSet diff currentMap.keys.toSet
          var valsOnlyInPrevMergedOutput = keysOnlyInPrevMergedOutput.map(key => mergedOutput(key))
          var keyValuesOnlyInPrevMergedOutput = keysOnlyInPrevMergedOutput.zip(valsOnlyInPrevMergedOutput).toMap

          var newOutput = currentMap ++ keyValuesOnlyInPrevMergedOutput
          mergedOutput = newOutput
        }
        else if(mergedDate.isAfter(currentDate))
        {
          var keysOnlyInCurrentMap = currentMap.keys.toSet diff mergedOutput.keys.toSet
          var valsOnlyInCurrentMap = keysOnlyInCurrentMap.map(key=>currentMap(key))
          var keyValuesOnlyInCurrentMap = keysOnlyInCurrentMap.zip(valsOnlyInCurrentMap).toMap

          var newOutput = mergedOutput ++ keyValuesOnlyInCurrentMap
          mergedOutput = newOutput
        }
      }
      println(mergedOutput)
      mergedOutput
    }
  }
}




