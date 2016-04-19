import scala.io.Source

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.log4j.Logger
import org.apache.log4j.Level

import edu.colorado.cs.datamining.utils._

object Correlation {

  var headerMap: Map[String, Int] = Map()

  // (education, (Education2003 range))
  val edu2003Range = Map(
    "higherEdu" -> (4, 8),
    "noHigherEdu" -> (1, 3))

  // (education, (Education1989 range))
  val edu1989Range = Map(
    "higherEdu" -> (9, 17),
    "noHigherEdu" -> (0, 12))

  // (ageCategory, (ageRecode27 range))
  val ageRange = Map(
    "young" -> (1, 13),
    "middle" -> (14, 17),
    "old" -> (14, 26))

  // (race, (HispOriginRecode range))
  val raceRange = Map(
    "white" -> (1, (6, 9)),
    "hispanic" -> (1, (1, 5)),
    "black" -> (2, (6, 9)),
    "native american" -> (3, (6, 9)),
    "chinese" -> (4, (6, 9)),
    "japanese" -> (5, (6, 9)))

    /* Opens header csv and returns map of (name, column #) 
  def mapHeaderIndexes(headerFileName: String) : Map[String, Int] = {
    val header = scala.io.Source.fromFile(headerFileName).mkString
    var headerMap: Map[String, Int] = Map()
    header.split(",").view.zipWithIndex.foreach{case (name, index) => headerMap += (name.stripLineEnd -> index)} //http://daily-scala.blogspot.com/2010/05/zipwithindex.html

    return headerMap
  }
  */

  def checkMatch(entry : Array[String], feature : Array[String]) : Boolean = {

    //println("hispanic = " + entry(headerMap("HispanicOriginRaceRecode").toInt))
    //return false

    // Age
    val ageRangeTuple = ageRange(feature(1))
    val entryAge = entry(headerMap("AgeRecode27")).toInt
    val ageMatch = entryAge >= ageRangeTuple._1 && entryAge <= ageRangeTuple._2

    // Race
    val raceRangeTuples = raceRange(feature(2))
    val raceType = entry(headerMap("Race")).toInt
    val hispanicOrigin = entry(headerMap("HispanicOriginRaceRecode")).toInt

    val raceMatch = raceType == raceRangeTuples._1 &&
      hispanicOrigin >= raceRangeTuples._2._1 &&
      hispanicOrigin <= raceRangeTuples._2._2

    // Education
    val eduType = entry(headerMap("EducationReportingFlag")).toInt
    var eduCode = entry(headerMap("Education2003Revision")).toInt
    var eduMap = edu2003Range
    
    if (eduType == 0) {
      eduMap = edu1989Range
      eduCode = entry(headerMap("Education1989Revision")).toInt
    }

    val eduRangeTuple = eduMap(feature(3))
    val eduMatch = eduCode >= eduRangeTuple._1 && eduCode <= eduRangeTuple._2

    // Sex
    val sexMatch = entry(headerMap("Sex")) == feature(4)

    return ageMatch && raceMatch && eduMatch & sexMatch
  }

  def parseFeatureFile(featureFile: String) {
    val f = Source.fromFile(featureFile).getLines
  }


  def conditionalProbability(dataFile: String,
                             headerFile: String,
                             featureFile: String,
                             cause: String) {

    println("in function")

    val conf = new SparkConf().setAppName("Conditional Probability")
    val sc = new SparkContext(conf)

    val dataRDD = sc.textFile(dataFile)
    headerMap  = mapHeaderIndexes(headerFile)
    println(featureFile)
    val features = Source.fromFile(featureFile).mkString.split("\n")
    val splitData = dataRDD.map(line => line.split(',')).cache()    

    for (feature <- features) {
      println(feature)

      //P(cause | demographic)
      val numFeature = splitData.filter{line =>
        checkMatch(line, feature.split(","))
      }.count()

      val numFeatureAndCause = splitData.filter{line =>
        checkMatch(line, feature.split(",")) &&
        line(headerMap("MannerOfDeath")).contains(cause)
      }.count()

      println("numFeature = %d numFeatureAndCause = %d".format(numFeature, numFeatureAndCause))
      println("Cond Prob = %f\n".format(numFeatureAndCause/numFeature.toDouble))
    }

    sc.stop()
  }

  def main(args: Array[String]) {

    //Turn off obnoxious logging
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val dataFile: String = args(0)
    val headerFile: String = args(1)
    val featureFile: String = args(2)

    
    conditionalProbability(dataFile, headerFile, featureFile, "3")
  }
}