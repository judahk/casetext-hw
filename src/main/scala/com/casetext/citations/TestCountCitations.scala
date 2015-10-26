package com.casetext.citations

/**
 * Created by judahk on 10/1/2015.
 */
object TestCountCitations {
  def main (args: Array[String]) {
    val dataDirPath = args(0)
    val reportersFilePath = args(1)
    val outputDirPath = args(2)
    val cc = new CountCitations(reportersFilePath)
    cc.countCitations(dataDirPath, outputDirPath)
  }
}
