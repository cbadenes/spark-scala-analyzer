package es.upm.oeg.spark.scala.analyzer

import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.EnableAutoConfiguration
import org.springframework.context.annotation.{Configuration, ComponentScan}

@Configuration
@EnableAutoConfiguration
@ComponentScan
object AppConfig {

  def main(args: Array[String]) {

    // Initialize Spring Context
    def context = SpringApplication.run(classOf[LinesCounter]);

    // Get LinesCounter Bean
    def linesCounter = context.getBean(classOf[LinesCounter]);

    // Count lines
    linesCounter.count();


  }

}
