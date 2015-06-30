package com.instructure

import net.liftweb.json._
import org.apache.spark.rdd._
import org.apache.spark.{SparkContext, _}
import org.joda.time._
import org.joda.time.format.DateTimeFormat

object DataMigration {

  def run(sc: SparkContext, inputPath: String, outputPath: String): Unit = {

    val textFile = sc.textFile(inputPath)
    val keyValuePairs = groupByRequestId(textFile)
    writeToFile(keyValuePairs, outputPath)
  }

  def groupByRequestId(textFile: RDD[String]): RDD[(String, Iterable[String])] = {
    val requestByIdValue = textFile.map { line =>
      val index = line.indexOf(" ")
      val requestId = line.substring(0, index)
      val json = line.substring(index)
      (requestId, json)
    }
    val keyValuePairs = requestByIdValue.groupByKey(numPartitions = 60)
    keyValuePairs
  }

  def writeToFile(keyValuePairs: RDD[(String,Iterable[String])], outputPath: String) = {
    keyValuePairs.map { case (requestId, requests) =>
      if (requests.size > 1) {
        val requestMap = merging(requests)
        if (requestMap.contains("created_at")) {
          val currentDate = DateTime.parse(requestMap("created_at"), DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ssZ"))
          requestMap("user_id") + "|" + currentDate + "|" + requestId + " " + mergedOutputToString(requestMap)
        }
      }
      else {
        val requestMap=parseStringToMapLift(requests.mkString)
        if(requestMap.contains("created_at")) {
          val currentDate = DateTime.parse(requestMap("created_at"), DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ssZ"))
          requestMap("user_id") + "|" + currentDate + "|" + requestId + requests.mkString
        }
      }
    }.saveAsTextFile(outputPath)
  }

  def merging(requests: Iterable[String]): Map[String, String] = {
    val requestMaps = requests.map(request => parseStringToMapLift(request))

    val datePattern = "yyyy-MM-dd HH:mm:ssZ"
    var mergedOutput = Map(("updated_at", "2000-05-20 15:53:25+0000"))

    for (currentMap <- requestMaps) {
      if (currentMap.contains("updated_at")) {
        //if(checkFor(currentMap,"updated_at")) {
          val currentDate = DateTime.parse(currentMap("updated_at"), DateTimeFormat.forPattern(datePattern))
          val mergedDate = DateTime.parse(mergedOutput("updated_at"), DateTimeFormat.forPattern(datePattern))

          if (currentDate.isAfter(mergedDate)) {
            val keysOnlyInPrevMap = mergedOutput.keys.toSet diff currentMap.keys.toSet
            val keyValuesOnlyInPrevMap = keysOnlyInPrevMap.map(k => (k, mergedOutput(k))).toMap
            val newOutput = currentMap ++ keyValuesOnlyInPrevMap
            mergedOutput = newOutput
          }
          else if (mergedDate.isAfter(currentDate)) {
            val keysOnlyInCurrentMap = currentMap.keys.toSet diff mergedOutput.keys.toSet
            val keyValuesOnlyInCurrentMap = keysOnlyInCurrentMap.map(k => (k, currentMap(k))).toMap
            val newOutput = mergedOutput ++ keyValuesOnlyInCurrentMap
            mergedOutput = newOutput
          }
      }
        //}
    }
    mergedOutput
  }

  def checkFor(m: Map[String,String], s: String ) :Boolean = {
    if (m.contains(s))
      true
    else
      false
  }

  def parseStringToMapLift(s: String): Map[String, String] = {
    val m =parse(s).values.asInstanceOf[Map[String,String]]
    m
  }

  def mergedOutputToString(m: Map[String,String]) :String = {
    implicit val formats = net.liftweb.json.DefaultFormats
    val json = Extraction.decompose(m)
    Printer.compact(JsonAST.render(json))
  }

  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("DataMigration").set("spark.hadoop.validateOutputSpecs", "false"))
    //val inputPath = "/Users/pteeka/IdeaProjects/DataMig/src/test/resources/mergingTestFile.txt"
    //val outputPath="/Users/pteeka/IdeaProjects/DataMig/target/mergingTestFileOutput"
    run( sc, args(0),args(1));
    //run(sc,inputPath,outputPath)
  }
}







