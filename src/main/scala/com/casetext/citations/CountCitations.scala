package com.casetext.citations

import java.io.File

import org.apache.commons.io.{FileUtils, FilenameUtils}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileStatus, FileSystem}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._
import scala.io.Source
import scala.util.matching.Regex

/**
 * Created by judahk on 10/1/2015.
 */
class CountCitations(reportersFile: String) extends java.io.Serializable{
  val reporters = FileUtils.readLines(new File(reportersFile)).map(s => s.toString)
  val reportersEscaped = reporters.map(escapeRegexChars)
  val reportersRegexString = "(?:" + reportersEscaped.mkString("|") + ")"

  val citationRegexString = "\\b(\\d+\\s+" + reportersRegexString + ")\\s+(\\d+)\\b"
  val CitationRegex = citationRegexString.r

  val shortCitationRegexString = "\\b(\\d+\\s+" + reportersRegexString + ")(,?\\s+at\\s+)(\\d+)\\b"
  val ShortCitationRegex = shortCitationRegexString.r

  val pinciteRegexString = "\\b(\\d+\\s+" + reportersRegexString + "\\s+\\d+),\\s*\\d+\\b"
  val PinciteRegex = pinciteRegexString.r

  // regex to catch Id. citations so as to map them back to the previous citation
  val IdemRegexString = """Id.,?\s+at\s+\d+"""
  val IdemRegex = IdemRegexString.r

  val finalRegexString = citationRegexString + "|" + shortCitationRegexString + "|" + pinciteRegexString // + "|" + IdemRegexString
  val FinalRegex = finalRegexString.r

  val PageNumberRegex = "(?:P|p)age\\s+\\d+".r

  def countCitations(dataDirPath:String, outputDirPath: String) : Unit = {
    // Create SPARK conf and context
    val conf = new SparkConf().setAppName("CountCite").setMaster("local[*]").set("spark.local.dir", "C:\\tmp")
    //    conf.set("spark.eventLog.enabled", "true")
    //    conf.set("spark.eventLog.dir", "C:\\tmp\\eventLog.out")
    val sc = new SparkContext(conf)

    val st = System.currentTimeMillis()

    // Read all files using wholeTextFiles because we want all files to be separate
//    val fileContentRDD = sc.wholeTextFiles(dirPath, 2048) //Try parallelizing this read to make it fast, its quite slow!!

    // Read all files in parallel for speed up (much faster than using wholeTextFiles!!)
    val paths = List(dataDirPath)
    val pathsRDD = sc.parallelize(paths) // Convert list of paths into an RDD for parallel processing
    val filenamesRDD = pathsRDD.flatMap(list_file_names).setName("filenamesRDD") //.cache()
    val filenamesRDDSize = filenamesRDD.count().toInt
    val fileContentRDD = filenamesRDD.repartition(filenamesRDDSize).map(read_file).setName("fileContentRDD") //.cache()

    // Extract and count citations for each file and output the result to outputDirPath
    val citationsRDD = fileContentRDD.flatMap(extractAndCountCitations).setName("citationsRDD") //.cache()
    citationsRDD.map(t => t._1 + ", " + t._2 + ", " + t._3).repartition(50).saveAsTextFile(outputDirPath)
//    citationsRDD.foreach(myPrintln)

    val et = System.currentTimeMillis()
    val tot = et - st
    println("Total time: " + tot)
    sc.stop()
  }

  def escapeRegexChars(name: String) : String = { // escape chars that are used as part of reg expressions
    val regexChars = List("^","$",".","[","]","\\","*","+","?","{","}","|","(",")",":",">")
    val res = name.map(c => {
      if (regexChars.contains(c.toString)){
        "\\" + c
      } else {
        c
      }
    }).mkString
    res
  }

  def myPrintln(args: (String,String,Int)) : Unit = {
    val (filepath, contents, sum) = args
    println(filepath + ", " + contents + ", " + sum)
  }

  def extractAndCountCitations(args: (String, String)) : List[(String,String,Int)] = {
    val (filepath, contents) = args
    val filename = FilenameUtils.getName(filepath)
    val contentsWithPageNumbersRemoved = PageNumberRegex.replaceAllIn(contents, "") // Remove text content of the <page_number> tag, if any
    val citations = findCitationsViaRegex(contentsWithPageNumbersRemoved) // Find all citations in the file content
//    val citationsNoIdem = citations(0) :: citations.sliding(2).map(mapIdemToRecentCitation).toList // Map Id. to its previous citation (need more work here!!)
    val longCitations = citations.filter(isLongCitation) // Filter out short citations (of the form "555 U.S., at 312")
    val mainCitations = longCitations.map(removePinCites) // Remove pincites from citations

    // Group main citations by vol & reporter and associate the list of start pages with each key (key = vol & reporter)
    val citationsMap = mainCitations.groupBy(getVolAndRepFromCitation).mapValues(lv => lv.map(getStartPageFromCitation))

    // Create a function to map short citations to correct main citations and do the mapping
    val shortCitationMapperFunc = createShortCitationMapper(citationsMap, filename)
    val newCitations = citations.map(shortCitationMapperFunc) // Map short citations back to correct main citation

    //group same citations together, then count each and return the result
    val newCitationsMap = newCitations.groupBy(s => s)
    val filenameCitationsCounts = newCitationsMap.keys.toList.map(key => (filename, key, newCitationsMap(key).length))
    return filenameCitationsCounts
  }

  def mapIdemToRecentCitation(args: List[String]) : String = args(1) match {
    case IdemRegex => args(0)
    case _ => args(1)
  }

  def isLongCitation(citation: String) : Boolean = citation match {
    case ShortCitationRegex(_, _, _) => false
    case _ => true
  }

  def getVolAndRepFromCitation(citation: String) : String = citation match {
    case CitationRegex(volAndRep, _) => volAndRep
//    case _ => citation
  }

  def getStartPageFromCitation(citation: String) : String = citation match {
    case CitationRegex(_, startPage) => startPage
//    case _ => citation
  }

  def removePinCites(citation: String) : String = citation match {
    case PinciteRegex(mainCitation) => mainCitation
    case _ => citation
  }

  def findCitationsViaRegex(line: String) : List[String] = { // Find all citations and return as a list of strings
    val matches =  FinalRegex.findAllIn(line)
    return matches.toList
  }

  def createPair(firstArg: String) = (secondArg: String) => {
    firstArg + " " + secondArg
  }

  def createPair2(firstArg: String) = (secondArg: String) => {
    (firstArg, secondArg)
  }

  // This function is a simple way to map a short citation to a correct main citation. For a given short citation, we find its 'key' (i.e., vol & reporter)
  // and look for all start pages corresponding to this key. Given this list of start pages, we use a simple linear search to find the correct start page, which
  // determines the correct main citation to which this short citation should be mapped. (See also the comment above the method "getCorrectStartPage")
  def createShortCitationMapper(mainCitationMap:  scala.collection.immutable.Map[String,List[String]], filename: String) = (shortCitation: String) => shortCitation match {
    case ShortCitationRegex(volAndRep, _, pgNum) => {
      val key = volAndRep
      val pageNum = pgNum.toInt
      mainCitationMap.get(key) match {
        case Some(startPages) => {
          //Map shortCitation to the correct starting page
          val correctStartPage = getCorrectStartPage(startPages.map(e => e.toInt), pageNum)
          if (correctStartPage != -1){
            val correctCitation = volAndRep + " " + correctStartPage
            correctCitation
          }else{
            println("ERROR: short citation's page does not map to any previous citation, short citation: "
              + shortCitation + ", page:" + pageNum + ", filename: " + filename)
            ""
          }
        }
        case None => {
          println("ERROR: short citation's volume and reporter does not match any previous citation," +
            " short citation: " + shortCitation + ", volume & reporter: " + key + ", filename: " + filename)
          ""
        }
      }
    }
    case _ => shortCitation
  }

  // Find and return the start page p such that p <= page < p', where p' is the next start page
  // in the sorted order of start pages (where all start pages correspond to a specific vol & reporter)
  def getCorrectStartPage(startPages: List[Int], page: Int) : Int = {
    if (page >= startPages.max){
      return startPages.max
    }else{
      val iterator = startPages.sorted.sliding(2).filter(x => x(0) <= page && page < x(1))
      if (!iterator.hasNext)
        return -1 //For now return -1 if incorrect short citation is found, need a better way to handle this
      val correctStartPage = iterator.next.head
      correctStartPage
    }
  }

  def createKeyValPairs(args: (String, String)) : Tuple2[(String, String), Int] = {
    return new Tuple2[(String, String), Int](args, 1)
  }

  // Method to list all file names below a dir path
  def list_file_names(path: String): Seq[String] = {
    val fs = FileSystem.get(new File(path).toURI, new Configuration())
    def f(path: Path): Seq[String] = {
      Option(fs.listStatus(path)).getOrElse(Array[FileStatus]()).
        flatMap {
        case fileStatus if fileStatus.isDirectory => f(fileStatus.getPath)
        case fileStatus => Seq(fileStatus.getPath.toString)
      }
    }
    f(new Path(path))
  }

  def read_file(path: String): Tuple2[String, String] = {
    val fs = FileSystem.get(new File(path).toURI, new Configuration())
    val file = fs.open(new Path(path))
    val source = Source.fromInputStream(file, "UTF-8")
    val lines = source.getLines.toList
    val content = lines.reduce((l1, l2) => l1 + "\n" + l2)
    return new Tuple2(path, content)
  }
}
