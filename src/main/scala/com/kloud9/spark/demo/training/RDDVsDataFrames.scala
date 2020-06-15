package com.kloud9.spark.demo.training

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object RDDVsDataFrames {

  def main(args: Array[String]): Unit = {

    //Setup Logging
    Logger.getLogger("org").setLevel(Level.ERROR)

    //Constants
    val dataroute = "/Users/nithya/work/gitrepos/spark_batch_processing/src/main/scala/com/kloud9/spark/demo/data/raw/"

    //Setup Spark Session
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("RDDvsDataFrames")
      .getOrCreate()

    //Reading a File using Spark Context by Default will produce the RDD, And RDD is a Array of String, And It is unstructured, No Schema

    println("RDD ====================== Start")

    val salesRDD = spark.sparkContext.textFile("file:///"+ dataroute+"sales.csv")

    salesRDD.count()



    salesRDD.take(10).foreach(println)

    println("RDD ====================== End")

    println()
    println()
    println()
    println()


    //Reading a File using Spark Session by Default will produce the DataFrame, DataFrame is the Structure Data with Columns and Rows attached with Schema

    println("DataFrames ====================== start")
    val salesDF = spark.read.format("csv").option("header","true").option("inferSchema","true").load("file:///"+ dataroute+"sales.csv")

    //Count of Entire Data Frame by Applying Count Action
    println(salesDF.count())
    salesDF.show(10)

    salesDF.printSchema()
    println("DataFrames ====================== End")

  }

}
