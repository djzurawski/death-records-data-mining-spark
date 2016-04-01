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


  def fracByCategoryAndManner(headerFile:String, dataFile:String, category:String, mannerKey:String) {

     //Setup spark contect
    val conf = new SparkConf().setAppName("suicideFracByKeyAndSex")
    val sc = new SparkContext(conf)


    val dataRDD = sc.textFile(dataFile)
    val headerMap: Map[String, Int] = mapHeaderIndexes(headerFile)

    val splitData = dataRDD.map(line => line.split(',')).cache()

    println("created split data")

    val categoryPairs = splitData.map(line => (line(headerMap(category)), 1))
    val categoryDeathCounts = categoryPairs.reduceByKey((a,b) => a + b)

    println("reduced by key")

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

  def filter1PerSex(headerFile:String, dataFile:String,
                                 cat1:String, cat1Key:String,
                                  binCat:String, mannerKey:String) {

    //Setup spark contect
    val appName =  "%s:%s : Manner:%s, Bin:%s".format(cat1, cat1Key, mannerKey, binCat)
    val conf = new SparkConf().setAppName(appName)
    val sc = new SparkContext(conf)

    println("opening file")

    val dataRDD = sc.textFile(dataFile)
    val headerMap: Map[String, Int] = mapHeaderIndexes(headerFile)

    println("read data")

    val splitData = dataRDD.map(line => line.split(',')).cache()

    //val binPairs = splitData.map(line => (line(headerMap(binCat)), 1))
    //val totalInBins = binPairs.reduceByKey((a,b) => a + b)

    println("created split data")


    // All who were in category
    val totalMen = splitData.filter{line =>
      line(headerMap("Sex")).contains("M") &&
      line(headerMap("MannerOfDeath")).contains(mannerKey)       
    }

    //Reduce to values of (binKey, num in binKey)
    val menBinPairs = totalMen.map(line => (line(headerMap(binCat)), 1))
    val totalMenBinned = menBinPairs.reduceByKey((a,b) => a + b)

    // Fit category and died by Manner
    val menFiltered = totalMen.filter{line =>
      line(headerMap(cat1)).contains(cat1Key)     
    }

    println("filtered")

    val menFilteredPairs = menFiltered.map(line => (line(headerMap(binCat)), 1)) //filter age here headerman(age)
    val menFilteredCounts = menFilteredPairs.reduceByKey((a,b) => a + b) //age

    println("reduced by key")

    val menCombinedCounts = menFilteredCounts.join(totalMenBinned)


    
    println(appName)
    println("binning=" + binCat + ", filtered/total deaths")
    //menFilteredCounts.collect().foreach{line => println(line)}

    menCombinedCounts.collect().foreach{case (bin, (filtered, total)) =>
     // println(bin + ", " + filtered.toDouble/total.toDouble)
      println("%d, %f : %d %d".format(bin.toInt, filtered/total.toDouble, filtered, total))
    }
    println()
    
  }



  def filter2PerSex(headerFile:String, dataFile:String,
                                 cat1:String, cat1Key:String,
                                 cat2:String, cat2Key:String,
                                  binCat:String, mannerKey:String) {

    //Setup spark contect
    val appName =  "%s,%s : %s,%s : %s".format(cat1, cat1Key, cat2, cat2Key, binCat)
    val conf = new SparkConf().setAppName(appName)
    val sc = new SparkContext(conf)

    val dataRDD = sc.textFile(dataFile)
    val headerMap: Map[String, Int] = mapHeaderIndexes(headerFile)

    val splitData = dataRDD.map(line => line.split(',')).cache()

    val totalMenDeaths = splitData.filter{line => line(headerMap("Sex")).contains("M")}
    val totalWomenDeaths = splitData.filter{line => line(headerMap("Sex")).contains("F")}

    val menFiltered = totalMenDeaths.filter{line =>
      line(headerMap(cat1)).contains(cat1Key) &&
      line(headerMap(cat2)).contains(cat2Key) &&
      line(headerMap("MannerOfDeath")).contains(mannerKey)
    }

    val menFilteredPairs = menFiltered.map(line => (line(headerMap(binCat)), 1)) //filter age here headerman(age)
    val menFilteredCounts = menFilteredPairs.reduceByKey((a,b) => a + b) //age
    val menCombinedCounts = menFilteredCounts.join(menFilteredCounts)


    println(appName)
    println("binning=" + binCat + ", bin/total deaths")
    menFilteredCounts.collect().foreach{line => println(line)
      //println(bin + ", " + filtered.toDouble/total.toDouble)
    }
    println()
  }




  def main(args: Array[String]) {

    //Turn off obnoxious logging
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val headerFile: String = args(1)
    val dataFile: String = args(0)

    filter1PerSex(headerFile, dataFile, "MaritalStatus", "D", "AgeRecode27","2")
    println("finished")
  }
}