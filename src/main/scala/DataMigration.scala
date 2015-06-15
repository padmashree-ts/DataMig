package com.instructure

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, _}
import org.joda.time._
import org.joda.time.format.DateTimeFormat
import play.api.libs.json.{Json, _}
import org.apache.spark.rdd.PairRDDFunctions
import scala.reflect.ClassTag

object DataMigration {
  def run(sc: SparkContext, inputPath: String, outputPath: String) : Unit  = {
    val textFile=sc.textFile(inputPath)
    val requestByIdValue = textFile.map(textFile => {
      val index = textFile.indexOf(" ")
      val requestId = textFile.substring(0, index)
      val json = textFile.substring(index)
      (requestId, json)
    })

    implicit def rddToPairRDDFunctions[K: ClassTag, V: ClassTag](rdd: RDD[(K, V)]) = new PairRDDFunctions(rdd)

    val pairRequestByIdValue=rddToPairRDDFunctions(requestByIdValue)

    val keyValuePairs = pairRequestByIdValue.groupByKey()
    keyValuePairs.map { case (requestId, requests) =>
      if (requests.size > 1) {
        requestId + " " + mergedOutputToString(merging(requests))
      }
      else {
        requestId + requests.mkString
      }
    }.saveAsTextFile(outputPath)

    def parseJsonToMap(s: String): Map[String, String] = {
      Json.parse(s).as[JsObject].fields.map { case (k, v) => (k, v.as[String]) }.toMap
    }

    def merging(requests: Iterable[String]): Map[String,String] = {
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

  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("DataMigration").setMaster("local").set("spark.hadoop.validateOutputSpecs", "false"))
    //val inputPath ="/Users/pteeka/IdeaProjects/DataMig/src/main/resources/lessDataMigration.txt"
    //val outputPath="/Users/pteeka/IdeaProjects/DataMig/target/lessDataMigration"
    run( sc, args(0),args(1));
    //run(sc,inputPath,outputPath)
  }
}






