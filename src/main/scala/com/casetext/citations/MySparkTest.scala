package com.casetext.citations

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by judahk on 10/1/2015.
 */
object MySparkTest {
  def main(args: Array[String]) {
    val input = args(0)
//    val output = args(1)
    val conf = new SparkConf().setAppName("MyTest") //.setJars(List("C:\\Users\\judahk\\workspace\\casetext_hw\\casetext-app\\target\\casetext-app-1.0-SNAPSHOT.jar")) //.setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.addJar("C:\\Users\\judahk\\workspace\\casetext_hw\\casetext-app\\target\\casetext-app-1.0-SNAPSHOT.jar")
    val dataRDD = sc.textFile(input)
    dataRDD.foreach(p => println(p))
    sc.stop()
  }
}
