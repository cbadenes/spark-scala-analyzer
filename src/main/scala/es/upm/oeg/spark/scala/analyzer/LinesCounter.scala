package es.upm.oeg.spark.scala.analyzer

import java.io.{File, StringReader}

import au.com.bytecode.opencsv.CSVReader
import es.upm.oeg.spark.scala.analyzer.CSVEntry._
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component


@Component
class LinesCounter{

  import es.upm.oeg.spark.scala.analyzer.CSVEntry.COLUMNS

  def LOG = LoggerFactory.getLogger(classOf[LinesCounter])

  // Attributes
  var folder = "src/main/config/provider"

  // Initialize Spark Context
  val conf  = new SparkConf().setMaster("local").setAppName("My Line Counter")
  val sc    = new SparkContext(conf)
  LOG.info("Spark Context initialized successfully");


  def analyze(){

    LOG.info("----------------------------------------------")

    // Read provider folder
    def providerFolder = new File(folder)

    // Analyze provider
    var publications  = 0L
    providerFolder.listFiles().foreach(path =>
      publications += analyzeProvider(path)
    )

    // Show results
    LOG.info("Total publications: {}", publications)
    LOG.info("Total providers: {}", providerFolder.listFiles().size)
  }


  def analyzeProvider(f: File): Long ={

    val input = sc.textFile(f.getAbsolutePath)

    val inputCSV = input.map{ line =>
      val reader = new CSVReader(new StringReader(line));
      reader.readNext;
    }

    // Count publications
    def publications = inputCSV.count()-1
    LOG.info("{}: {} publications", f.getName  , publications)

    def noCreatorsRDD = inputCSV.filter { pub =>
      val times       = pub(COLUMNS(CREATOR_AS_TEXT).toInt) + pub(COLUMNS(CREATOR_AS_URI).toInt)
      !times.equalsIgnoreCase(CREATOR_AS_TEXT+CREATOR_AS_URI) && times.toInt == 0
    }

    def noCreators    = noCreatorsRDD.count()
    LOG.info("{}: {} without creators", f.getName  , noCreators)

    return publications
  }

}
