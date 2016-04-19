package edu.colorado.cs.datamining

package object utils {

  case class Entry(
    id: Int,
    edu1989: Int,
    edu2003: Int,
    eduFlag: Int,
    monthOfDeath: Int,
    sex: String,
    age: Int,
    ageBin52: Int,
    ageBin27: Int,
    maritalStatus: String,
    manner: String,
    causeCode: String,
    causeBin352: Int,
    caseBin113: Int,
    causeBin39: Int,
    race: Int,
    hispanicCode: Int)

  val oldHeaderMap = Map(
    "id" -> 0,
    "edu1989" -> 1,
    "edu2003" -> 2,
    "eduFlag" -> 3,
    "sex" -> 5,
    "age" -> 6,
    "ageBin52" -> 8,
    "ageBin27" -> 9,
    "maritalStatus" -> 13,
    "manner" -> 17,
    "causeCode" -> 22,
    "causeBin352" -> 23,
    "causeCode113" -> 24,
    "causeCode39" -> 25,
    "race" -> 109,
    "hispanicCode" -> 115)

  def lineToEntry(line: String, headerMap: Map[String, Int]) : Entry = {
    val splitLine = line.split(",")

    val entry = Entry(
      splitLine(headerMap("id")).toInt,
      splitLine(headerMap("edu1989")).toInt,
      splitLine(headerMap("edu2003")).toInt,
      splitLine(headerMap("eduFlag")).toInt,
      splitLine(headerMap("monthOfDeath")).toInt,
      splitLine(headerMap("sex")).toString,
      splitLine(headerMap("age")).toInt,
      splitLine(headerMap("ageBin52")).toInt,
      splitLine(headerMap("ageBin27")).toInt,
      splitLine(headerMap("maritalStatus")).toString,
      splitLine(headerMap("manner")).toString,
      splitLine(headerMap("causeCode")).toString,
      splitLine(headerMap("causeBin352")).toInt,
      splitLine(headerMap("causeCode113")).toInt,
      splitLine(headerMap("causeCode39")).toInt,
      splitLine(headerMap("race")).toInt,
      splitLine(headerMap("hispanicCode")).toInt
      )

    return entry

  }

  /* Opens header csv and returns map of (name, column #) */
  def mapHeaderIndexes(headerFileName: String) : Map[String, Int] = {
    val header = scala.io.Source.fromFile(headerFileName).mkString
    var headerMap: Map[String, Int] = Map()
    header.split(",").view.zipWithIndex.foreach{case (name, index) => headerMap += (name.stripLineEnd -> index)} //http://daily-scala.blogspot.com/2010/05/zipwithindex.html

    return headerMap
  }


}