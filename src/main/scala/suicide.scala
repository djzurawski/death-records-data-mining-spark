import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors

import org.apache.log4j.Logger
import org.apache.log4j.Level

object Suicide {

  /* Opens header csv and returns map of (name, column #) */
  def mapHeaderIndexes(headerFileName: String) : Map[String, Int] = {
    val header = scala.io.Source.fromFile(headerFileName).mkString
    var headerMap: Map[String, Int] = Map()    
    header.split(",").view.zipWithIndex.foreach{case (name, index) => headerMap += (name -> index)} //http://daily-scala.blogspot.com/2010/05/zipwithindex.html

    return headerMap
  }

  def suicideAgeFracByKeyAndSex(headerFile : String, dataFile : String, key : String) {

     //Setup spark contect
    val conf = new SparkConf().setAppName("suicideFracByKeyAndSex")
    val sc = new SparkContext(conf)

    val dataRDD = sc.textFile(dataFile)
    val headerMap: Map[String, Int] = mapHeaderIndexes(headerFile)

    val splitData = dataRDD.map(line => line.split(',')).cache()

    val totalMenDeaths = splitData.filter{line => line(headerMap("Sex")).contains("M")}
    val totalWomenDeaths = splitData.filter{line => line(headerMap("Sex")).contains("F")}

    val menSuicides = splitData.filter{line =>
      line(headerMap("MannerOfDeath")).contains("2") &&
      line(headerMap("Sex")).contains("M")
    }

    val womenSuicides = splitData.filter{line =>
      line(headerMap("MannerOfDeath")).contains("2") &&
      line(headerMap("Sex")).contains("F")
    }


    val menKeyStatusPairs = totalMenDeaths.map(line => (line(headerMap(key)), 1))
    val menKeyStatusCounts = menKeyStatusPairs.reduceByKey((a, b) => a + b)

    val menSuicideKeyStatusPairs = menSuicides.map(line => (line(headerMap(key)), 1))
    val menSuicideKeyStatusCounts = menSuicideKeyStatusPairs.reduceByKey((a, b) => a + b)

    val menKeyStatusCombined = menKeyStatusCounts.join(menKeyStatusCounts)


    val womenKeyStatusPairs = totalWomenDeaths.map(line => (line(headerMap(key)), 1))
    val womenKeyStatusCounts = womenKeyStatusPairs.reduceByKey((a, b) => a + b)

    val womenSuicideKeyStatusPairs = womenSuicides.map(line => (line(headerMap(key)), 1))
    val womenSuicideKeyStatusCounts = womenSuicideKeyStatusPairs.reduceByKey((a, b) => a + b)

    val womenKeyStatusCombined = womenKeyStatusCounts.join(womenSuicideKeyStatusCounts)


    println("Men Suicides")
    println(key + ", suicides/total deaths")
    menKeyStatusCombined.collect().foreach{case (value, (total, suicide)) => 
      println(value + ", " + suicide.toDouble/total.toDouble)
    }
    println()

    println("Women Suicides")
    println(key + ", suicides/total deaths")
    womenKeyStatusCombined.collect().foreach{case (value, (total, suicide)) => 
      println(value + ", " + suicide.toDouble/total.toDouble)
    }
    println()    
  }


  def main(args: Array[String]) {

    //Turn off obnoxious logging
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val headerFile: String = args(1)
    val dataFile: String = args(0)

    suicideAgeFracByKeyAndSex(headerFile, dataFile, "Education2003Revision")



  }
}