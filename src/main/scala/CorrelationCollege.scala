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
    val entries = dataRDD.map(line => lineToEntry(line, collegeHeaderMap)).cache()
      
    for (feature <- features) {
      //println(feature)
      //P(cause | demographic)
      //println("numFeature = %d numFeatureAndCause = %d".format(numFeature, numFeatureAndCause))
      //println("Cond Prob = %f\n".format(numFeatureAndCause/numFeature.toDouble))

      val fractionMatches = year match {
        case -1 => {
          val numFeature = entries.filter{line =>
            checkMatch(line, feature.split(","))}.count()

          val numFeatureAndCause = entries.filter{line =>
            checkMatch(line, feature.split(",")) &&
            line.manner == cause}.count()

            numFeatureAndCause/numFeature.toDouble
        }
        case _ => {
          val numFeature = entries.filter{line =>
            checkMatch(line, feature.split(",")) &&
            line.year == year}.count()

          val numFeatureAndCause = entries.filter{line =>
            checkMatch(line, feature.split(",")) &&
            line.year == year &&
            line.manner == cause}.count()

            numFeatureAndCause/numFeature.toDouble
        }
      }
      println(fractionMatches)
    }

    sc.stop()
  }

  def countByCauseFeatures(dataFile: String,
                   featureFile: String,
                   year: Int,
                   binSize: Int) {

    val conf = new SparkConf().setAppName("Count by cause bin %d".format(binSize))
    val sc = new SparkContext(conf)

    val dataRDD = sc.textFile(dataFile)
    val features = Source.fromFile(featureFile).mkString.split("\n")
    val entries = dataRDD.map(line => lineToEntry(line, collegeHeaderMap)).cache()

    // FileWriter
    val file = new File("out.csv")
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write("binID, count, fraction\n")

    // for each feature
    for (feature <- features) {

      val yearMatches = year match {
        case -1 => entries.filter{checkMatch(_, feature.split(","))}
        case _  => entries.filter{entry => checkMatch(entry, feature.split(",")) && entry.year == year}
      }

      val featureMatchCount = yearMatches.count()

      val causeTuples = binSize match {
        case 39 => yearMatches.map(entry => (entry.causeBin39,1))
        case 113 => yearMatches.map(entry => (entry.causeBin113,1))
        case 352 => yearMatches.map(entry =>(entry.causeBin352,1))
      }

      val sortedCauseCounts = causeTuples
        .reduceByKey((a,b) => a + b)
        .sortBy(_._2, false) //sort values descending      

      println(feature)

      bw.write("%s\n".format(feature))
      sortedCauseCounts.collect().foreach{case (causeBin, count) => 
        bw.write("%d, %d, %f\n".format(causeBin, count, count/featureMatchCount.toDouble))
      }       
    }
    bw.close()
  }

  def countByCause(dataFile: String,
                   year: Int,
                   binSize: Int,
                   sc: SparkContext) {

    //val conf = new SparkConf().setAppName("Count by cause bin %d".format(binSize))
    //val sc = new SparkContext(conf)

    val dataRDD = sc.textFile(dataFile)
    val entries = dataRDD.map(line => lineToEntry(line, collegeHeaderMap)).cache()

    // FileWriter
    val file = new File("out%d.csv".format(year))
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write("binID, count, fraction\n")


    val yearMatches = year match {
      case -1 => entries.filter{entry => entry.sex == "M"}
      case _  => entries.filter{entry =>
        entry.year == year &&
        entry.sex == "M"}
    }

    val yearMatchCount = yearMatches.count()

    val causeTuples = binSize match {
      case 39 => yearMatches.map(entry => (entry.causeBin39,1))
      case 113 => yearMatches.map(entry => (entry.causeBin113,1))
      case 352 => yearMatches.map(entry =>(entry.causeBin352,1))
    }

    val sortedCauseCounts = causeTuples
      .reduceByKey((a,b) => a + b)
      .sortBy(_._2, false) //sort values descending      

    sortedCauseCounts.collect().foreach{case (causeBin, count) => 
      bw.write("%d, %d, %f\n".format(causeBin, count, count/yearMatchCount.toDouble))
    }       
    bw.close()
  }


  def countByDayMonth(dataFile: String) {
    val conf = new SparkConf().setAppName("Frac by Month")
    val sc = new SparkContext(conf)

    val dataRDD = sc.textFile(dataFile)
    val entries = dataRDD.map(line => lineToEntry(line, collegeHeaderMap)).cache()

    val monthCount = entries
      .filter(entry => entry.manner ==  "3")    
      .map(entry => (entry.monthOfDeath, 1))
      .reduceByKey((a,b) => a + b)
      .sortByKey()

    val dayCount = entries
      .filter(entry => entry.manner == "3" )    
      .map(entry => (entry.dayOfWeek, 1))
      .reduceByKey((a,b) => a + b)
      .sortByKey()

      println("Month")
      monthCount.collect().foreach{case (month, count) =>
        println("%d, %d".format(month,count))}

      println("days")
      dayCount.collect().foreach{case (day, count) => 
        println("%d, %d".format(day, count))}
  }

  def main(args: Array[String]) {

    //Turn off obnoxious logging
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val dataFile: String = args(0)
    val featureFile: String = args(1)

    //conditionalProbability(dataFile, featureFile, "0")
    //conditionalProbability(dataFile, featureFile, "1", -1)

    //val conf = new SparkConf().setAppName("Count by cause bin %d".format(binSize))
    //val sc = new SparkContext()

    //for (i <- 5 to 12) {
      //val year = 2000 + i
      //countByCause(dataFile, -1, 113, sc)
    //}


   countByDayMonth(dataFile)
  }
}