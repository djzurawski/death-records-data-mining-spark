import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors

import org.apache.log4j.Logger
import org.apache.log4j.Level

object ByManner {

  /* Opens header csv and returns map of (name, column #) */
  def mapHeaderIndexes(headerFileName: String) : Map[String, Int] = {
    val header = scala.io.Source.fromFile(headerFileName).mkString
    var headerMap: Map[String, Int] = Map()    
    header.split(",").view.zipWithIndex.foreach{case (name, index) => headerMap += (name -> index)} //http://daily-scala.blogspot.com/2010/05/zipwithindex.html

    return headerMap
  }

  def fracByCategoryAndManner(headerFile:String, dataFile:String, category:String, mannerKey : String) {

     //Setup spark contect
    val conf = new SparkConf().setAppName("suicideFracByKeyAndSex")
    val sc = new SparkContext(conf)

    val dataRDD = sc.textFile(dataFile)
    val headerMap: Map[String, Int] = mapHeaderIndexes(headerFile)

    val splitData = dataRDD.map(line => line.split(',')).cache()

    val categoryPairs = splitData.map(line => (line(headerMap(category)), 1))
    val categoryDeathCounts = categoryPairs.reduceByKey((a,b) => a + b)

    val allByManner = splitData.filter{line => line(headerMap("MannerOfDeath")).contains(mannerKey)}
    val categoryMannerPairs = allByManner.map(line => (line(headerMap(category)), 1))
    val categoryMannerCounts = categoryMannerPairs.reduceByKey((a,b) => a + b)

    val combinedDeathManner = categoryMannerCounts.join(categoryDeathCounts)

    println("Fraction MannerOfDeath by " + category)
    println("manner key=" + mannerKey + ", manner/total deaths")
    combinedDeathManner.collect().foreach{case (key, (manner, total)) => 
      println(key + ", " + manner.toDouble/total.toDouble)
    }
    println()
  }




  def main(args: Array[String]) {

    //Turn off obnoxious logging
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val headerFile: String = args(1)
    val dataFile: String = args(0)

    fracByCategoryAndManner(headerFile, dataFile, "Education2003Revision", "1")
  }
}