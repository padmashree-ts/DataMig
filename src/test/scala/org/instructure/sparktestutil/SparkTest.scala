package org.instructure.sparktestutil

import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FunSuite

import scala.collection.JavaConversions


object SparkTest extends org.scalatest.Tag("org.instructure.test.tags.SparkTest")

trait SparkTestUtils extends FunSuite {
  var sc: SparkContext = _

  /**
   * convenience method for tests that use spark.  Creates a local spark context, and cleans
   * it up even if your test fails.  Also marks the test with the tag SparkTest, so you can
   * turn it off
   *
   * By default, it turn off spark logging, b/c it just clutters up the test output.  However,
   * when you are actively debugging one test, you may want to turn the logs on
   *
   * @param name the name of the test
   * @param silenceSpark true to turn off spark logging
   */
  def sparkTest(name: String, silenceSpark: Boolean = true)(body: => Unit) {
    test(name, SparkTest) {
      val origLogLevels = if (silenceSpark) SparkUtil.silenceSpark() else null
      sc = new SparkContext(new SparkConf().setAppName(name).setMaster("local[4]").set("spark.hadoop.validateOutputSpecs", "false"))
      try {
        body
      }
      finally {
        sc.stop()
        sc = null
        // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
        System.clearProperty("spark.master.port")
        if (silenceSpark) SparkUtil.setLogLevels(origLogLevels)
      }
    }
  }
}

object SparkUtil {
  def silenceSpark():Map[String,org.apache.log4j.Level] =  {
    setLogLevels(Level.WARN, Seq("org.apache.spark", "remote", "org.eclipse.jetty", "io.netty", "org.apache.hadoop", "akka", "Utils", "Remoting", "RemoteActorRefProvider$RemotingTerminator"))
  }

  def setLogLevels(level: org.apache.log4j.Level, loggers: TraversableOnce[String]):Map[String,org.apache.log4j.Level] = {
    loggers.map {
      loggerName =>
        val names = JavaConversions.enumerationAsScalaIterator(LogManager.getCurrentLoggers()).map(_.asInstanceOf[Logger].getName())
        val logger = Logger.getLogger(loggerName)
        val prevLevel = logger.getLevel
        logger.setLevel(level)
        loggerName -> prevLevel
    }.toMap
  }

  def setLogLevels(levels: Map[String,org.apache.log4j.Level]) = {
    levels.foreach {
      case (loggerName, level) => {}
        val logger = Logger.getLogger(loggerName)
        logger.setLevel(level)
    }
  }

}