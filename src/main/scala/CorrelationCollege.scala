import scala.io.Source
import java.io._

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

import org.apache.log4j.Logger
import org.apache.log4j.Level

import edu.colorado.cs.datamining.utils._

object CorrelationCollege {

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

  def checkMatch(entry : Entry, feature : Array[String]) : Boolean = {

    //println("hispanic = " + entry(headerMap("HispanicOriginRaceRecode").toInt))
    //return false

    // Race
    val raceRangeTuples = raceRange(feature(1))
    val raceType = entry.race
    val hispanicOrigin = entry.hispanicCode

    val raceMatch = raceType == raceRangeTuples._1 &&
      hispanicOrigin >= raceRangeTuples._2._1 &&
      hispanicOrigin <= raceRangeTuples._2._2

    // Education
    val eduType = entry.eduFlag
    var eduCode = entry.edu2003
    var eduMap = edu2003Range
    
    if (eduType == 0) {
      eduMap = edu1989Range
      eduCode = entry.edu1989
    }

    val eduRangeTuple = eduMap(feature(2))
    val eduMatch = eduCode >= eduRangeTuple._1 && eduCode <= eduRangeTuple._2

    // Sex
    val sexMatch = entry.sex == feature(3)

    return raceMatch && eduMatch & sexMatch
  }

  def parseFeatureFile(featureFile: String) {
    val f = Source.fromFile(featureFile).getLines
  }


  def conditionalProbability(dataFile: String,
                             featureFile: String,
                             cause: String,
                             year: Int) {

    println("in function")

    val conf = new SparkConf().setAppName("Conditional Probability College")
    val sc = new SparkContext(conf)

    val dataRDD = sc.textFile(dataFile)
    println(featureFile)
    val features = Source.fromFile(featureFile).mkString.split("\n")
    val collegeEntries = dataRDD.map(line => lineToEntry(line, collegeHeaderMap)).cache()
      
    for (feature <- features) {
      //println(feature)
      //P(cause | demographic)
      //println("numFeature = %d numFeatureAndCause = %d".format(numFeature, numFeatureAndCause))
      //println("Cond Prob = %f\n".format(numFeatureAndCause/numFeature.toDouble))

      if (year == -1) {
        println(calcFrac(collegeEntries,feature, cause))
      }
      else {
        println(calcFracByYear(collegeEntries,feature, cause, year))
      }
    }
    sc.stop()
  }


  def calcFrac(entires: RDD[Entry], feature: String, cause: String): Double = {
    //P(cause | demographic)
    val numFeature = entires.filter{line =>
      checkMatch(line, feature.split(","))
    }.count()

    val numFeatureAndCause = entires.filter{line =>
      checkMatch(line, feature.split(",")) &&
      line.manner == cause
    }.count()

    return numFeatureAndCause/numFeature.toDouble

  }

  def calcFracByYear(entires: RDD[Entry], feature: String, cause: String, year: Int): Double = {
    //P(cause | demographic)
    val numFeature = entires.filter{line =>
      checkMatch(line, feature.split(",")) &&
      line.year == year
    }.count()

    val numFeatureAndCause = entires.filter{line =>
      checkMatch(line, feature.split(",")) &&
      line.year == year &&
      line.manner == cause
    }.count()

    return numFeatureAndCause/numFeature.toDouble
  }

  def fracByCause39(dataFile: String,
                   featureFile: String,
                   year: Int) {

    val conf = new SparkConf().setAppName("Fract By Cause39")
    val sc = new SparkContext(conf)

    val dataRDD = sc.textFile(dataFile)
    println(featureFile)
    val features = Source.fromFile(featureFile).mkString.split("\n")
    val collegeEntries = dataRDD.map(line => lineToEntry(line, collegeHeaderMap)).cache()

    // FileWriter
    val file = new File("out.txt")
    val bw = new BufferedWriter(new FileWriter(file))


    // for each feature
    for (feature <- features) {
      println(feature)

      //filter entries by features
      val featureMatches = collegeEntries.filter{line =>
        checkMatch(line, feature.split(","))
      }

      //make tuples (cauesCode39, 1)
      val causeTuples = featureMatches.map(entry => (entry.causeBin39, 1))
      
      // Reduce by key
      val causeCounts = causeTuples.reduceByKey((a,b) => a + b)
      val sortedCauseCounts = causeCounts.sortByKey()

      sortedCauseCounts.collect().foreach{
        case (causeBin39, count) => 

        bw.write("%d, %d\n".format(causeBin39, count))

      }
          //println("%d, %d".format(causeBin39, count))}
    }
        bw.close()

  }

  def fracByCause113(dataFile: String,
                   featureFile: String,
                   year: Int) {

    val conf = new SparkConf().setAppName("Frac By Cause113")
    val sc = new SparkContext(conf)

    val dataRDD = sc.textFile(dataFile)
    println(featureFile)
    val features = Source.fromFile(featureFile).mkString.split("\n")
    val collegeEntries = dataRDD.map(line => lineToEntry(line, collegeHeaderMap)).cache()

    // FileWriter
    val file = new File("out.txt")
    val bw = new BufferedWriter(new FileWriter(file))

    // for each feature
    for (feature <- features) {
      println(feature)

      bw.write("%s\n".format(feature))

      val featureMatches = collegeEntries.filter{line =>
        checkMatch(line, feature.split(","))
      }
      val causeTuples = featureMatches.map(entry => (entry.causeBin113, 1))

      val causeCounts = causeTuples.reduceByKey((a,b) => a + b)
      val sortedCauseCounts = causeCounts.sortByKey()

      sortedCauseCounts.collect().foreach{
        case (cause, count) => 
          bw.write("%d, %d\n".format(cause, count))
      }
    }
    bw.close()
  }

  def main(args: Array[String]) {

    //Turn off obnoxious logging
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val dataFile: String = args(0)
    val featureFile: String = args(1)

    //conditionalProbability(dataFile, featureFile, "0")
    //conditionalProbability(dataFile, featureFile, "1", -1)
   fracByCause113(dataFile, featureFile, -1)
  }
}