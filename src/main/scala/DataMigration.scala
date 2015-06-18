package com.instructure

import org.apache.spark.rdd._
import org.apache.spark.{SparkContext, _}
import org.apache.spark.SparkContext._
import org.joda.time._
import org.joda.time.format.DateTimeFormat
import play.api.libs.json.{Json, _}

object DataMigration {

  def run(sc: SparkContext, inputPath: String, outputPath: String) : Unit  = {

    val textFile=sc.textFile(inputPath)
    val keyValuePairs=groupByRequestId(textFile)

    keyValuePairs.map { case (requestId, requests) =>
      if (requests.size > 1) {
        val requestMap = merging(requests)
        val currentDate = DateTime.parse(requestMap("created_at"),DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ssZ"))
        requestMap("user_id") + "|" + currentDate + "|" + requestId + " " + mergedOutputToString(requestMap)
      }
      else {
        val requestMap=parseJsonToMap(requests.mkString)
        val currentDate = DateTime.parse(requestMap("created_at"),DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ssZ"))
        requestMap("user_id") + "|" + currentDate + "|" + requestId + " " + requests.mkString
      }
    }.saveAsTextFile(outputPath)

    def parseJsonToMap(s: String): Map[String, String] = {
      Json.parse(s).as[JsObject].fields.map { case (k, v) => (k, v.as[String]) }.toMap
    }

    //def merging(requests: Iterable[String]): Map[String,String] = {
    def merging(requests: Iterable[String]): Map[String,String]= {
      val requestMaps = requests.map(request => parseJsonToMap(request))

      val datePattern = "yyyy-MM-dd HH:mm:ssZ"
      var mergedOutput = Map(("created_at", "2000-05-20 15:53:25+0000"))

      for (currentMap <- requestMaps) {
        val currentDate = DateTime.parse(currentMap("created_at").toString(), DateTimeFormat.forPattern(datePattern))
        val mergedDate = DateTime.parse(mergedOutput("created_at").toString(), DateTimeFormat.forPattern(datePattern))

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
      mergedOutput
    }

    def mergedOutputToString(m:Map[String,String]):JsValue = {
      Json.toJson(m)
      //ListMap(m.toSeq.sortBy(_._1):_*).map{case (k,v)=>pretty(k,v)}.mkString("{",",","}")
    }

    def pretty(k:String,v:String):String ={
      val quote="\""
      val s = quote +k+ quote + ":" + quote +v+ quote
      s
    }
  }

  def groupByRequestId(textFile: RDD[String]): RDD[(String,Iterable[String])] ={
    val requestByIdValue = textFile.map { line =>
      val index = line.indexOf(" ")
      val requestId = line.substring(0, index)
      val json = line.substring(index)
      (requestId, json)
    }
    val keyValuePairs=requestByIdValue.groupByKey()
    keyValuePairs
  }

  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("DataMigration").set("spark.hadoop.validateOutputSpecs", "false"))
    //val inputPath ="/Users/pteeka/IdeaProjects/DataMig/src/main/resources/lessDataMigration.txt"
    //val outputPath="/Users/pteeka/IdeaProjects/DataMig/target/lessDataMigration"
    run( sc, args(0),args(1));
    //run(sc,inputPath,outputPath)
  }
}






