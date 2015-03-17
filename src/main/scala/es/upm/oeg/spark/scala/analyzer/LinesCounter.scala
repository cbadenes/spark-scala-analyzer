package es.upm.oeg.spark.scala.analyzer

import org.apache.spark.{SparkContext, SparkConf}
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component


@Component
class LinesCounter {

  def LOG = LoggerFactory.getLogger(classOf[LinesCounter])

  LOG.info("Lines Counter initialized")

  def count(){

    val conf  = new SparkConf().setMaster("local").setAppName("My Line Counter")
    val sc    = new SparkContext(conf)

    LOG.info("Spark Context initialized successfully");

    // Count lines
  }


}
