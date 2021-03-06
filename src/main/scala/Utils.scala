package edu.colorado.cs.datamining

package object utils {

  case class Entry(
    edu1989: Int,
    edu2003: Int,
    eduFlag: Int,
    monthOfDeath: Int,
    sex: String,
    age: Int,
    ageBin52: Int,
    ageBin27: Int,
    maritalStatus: String,
    dayOfWeek: Int,
    year: Int,
    manner: String,
    causeCode: String,
    causeBin352: Int,
    causeBin113: Int,
    causeBin39: Int,
    race: Int,
    hispanicCode: Int)

  val nberHeaderMap = Map(
    "edu1989" -> 1,
    "edu2003" -> 2,
    "eduFlag" -> 3,
    "monthOfDeath" -> 4,
    "sex" -> 5,
    "age" -> 6,
    "ageBin52" -> 8,
    "ageBin27" -> 9,
    "maritalStatus" -> 13,
    "dayOfWeek" -> 14,
    "year" -> 15,
    "manner" -> 17,
    "causeCode" -> 22,
    "causeBin352" -> 23,
    "causeBin113" -> 24,
    "causeBin39" -> 26,
    "race" -> 109,
    "hispanicCode" -> 115)

  val kaggleHeaderMap = Map(
    "edu1989" -> 2,
    "edu2003" -> 3,
    "eduFlag" -> 4,
    "monthOfDeath" -> 5,
    "sex" -> 6,
    "age" -> 8,
    "ageBin52" -> 10,
    "ageBin27" -> 11,
    "maritalStatus" -> 15,
    "dayOfWeek" -> 16,    
    "year" -> 17,
    "manner" -> 19,
    "causeCode" -> 24,
    "causeBin352" -> 25,
    "causeBin113" -> 26,
    "causeBin39" -> 28,
    "race" -> 31,
    "hispanicCode" -> 37)

  val collegeHeaderMap = Map(
    "edu1989" -> 1,
    "edu2003" -> 2,
    "eduFlag" ->3,
    "monthOfDeath" -> 4,
    "sex" -> 5,
    "age" -> 6,
    "ageBin52" -> 7,
    "ageBin27" -> 8,
    "maritalStatus" -> 9,
    "dayOfWeek" -> 10,
    "year" -> 0,
    "manner" -> 11,
    "causeCode" -> 12,
    "causeBin352" -> 13,
    "causeBin113" -> 14,
    "causeBin39" -> 15,
    "race" -> 16,
    "hispanicCode" -> 17
    )


  def lineToEntry(line: String, headerMap: Map[String, Int]) : Entry = {
    val splitLine = line.split(",")

    val entry = Entry(
      splitLine(headerMap("edu1989")).toInt,
      splitLine(headerMap("edu2003")).toInt,
      splitLine(headerMap("eduFlag")).toInt,
      splitLine(headerMap("monthOfDeath")).toInt,
      splitLine(headerMap("sex")).toString,
      splitLine(headerMap("age")).toInt,
      splitLine(headerMap("ageBin52")).toInt,
      splitLine(headerMap("ageBin27")).toInt,
      splitLine(headerMap("maritalStatus")).toString,
      splitLine(headerMap("dayOfWeek")).toInt,
      splitLine(headerMap("year")).toInt,
      splitLine(headerMap("manner")).toString,
      splitLine(headerMap("causeCode")).toString,
      splitLine(headerMap("causeBin352")).toInt,
      splitLine(headerMap("causeBin113")).toInt,
      splitLine(headerMap("causeBin39")).toInt,
      splitLine(headerMap("race")).toInt,
      splitLine(headerMap("hispanicCode")).toInt
      )

    return entry

  }

  def entryToCsvLine(entry: Entry) : String = {

    val line = "%d,%d,%d,%d,%s,%d,%d,%d,%s,%d,%d,%s,%s,%d,%d,%d,%d,%d\n".format(
      entry.edu1989,
      entry.edu2003,
      entry.eduFlag,
      entry.monthOfDeath,
      entry.sex,
      entry.age,
      entry.ageBin52,
      entry.ageBin27,
      entry.maritalStatus,
      entry.dayOfWeek,
      entry.year,
      entry.manner,
      entry.causeCode,
      entry.causeBin352,
      entry.causeBin39,
      entry.race,
      entry.hispanicCode)

    return line

  }

  /* Opens header csv and returns map of (name, column #) */
  def mapHeaderIndexes(headerFileName: String) : Map[String, Int] = {
    val header = scala.io.Source.fromFile(headerFileName).mkString
    var headerMap: Map[String, Int] = Map()
    header.split(",").view.zipWithIndex.foreach{case (name, index) => headerMap += (name.stripLineEnd -> index)} //http://daily-scala.blogspot.com/2010/05/zipwithindex.html

    return headerMap
  }


}