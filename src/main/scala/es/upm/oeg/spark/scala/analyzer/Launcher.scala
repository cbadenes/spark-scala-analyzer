package es.upm.oeg.spark.scala.analyzer

import org.springframework.boot.SpringApplication


object Launcher {

  def main(args: Array[String]) {

    // Initialize Spring Context
    def context = SpringApplication.run(classOf[AppConfig]);

    // Get LinesCounter Bean
    def linesCounter = context.getBean(classOf[LinesCounter]);

    // Count lines
    linesCounter.analyze();


  }


}
