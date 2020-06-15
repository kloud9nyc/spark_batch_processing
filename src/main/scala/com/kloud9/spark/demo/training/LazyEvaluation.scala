package com.kloud9.spark.demo.training

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object LazyEvaluation {

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

    //Reading a File using Spark Session by Default will produce the DataFrame, DataFrame is the Structure Data with Columns and Rows attached with Schema

    val salesDF = spark.read.format("csv").option("header","true").option("inferSchema","true").load("file:///"+ dataroute+"sales.csv")

    //Count of Entire Data Frame by Applying Count Action
    println("Total Count is " + salesDF.count())

    //Transformation 1
    val CustomerNotNullDF = salesDF.where("CustomerID is Not Null")

    //Transformation 2
    val UnitPricessLessThan2DF = CustomerNotNullDF.where("UnitPrice < 2")

    //Transformation 3
    val QuantitytyMorethan1000DF = UnitPricessLessThan2DF.where("Quantity > 1000")

    //Transformation 4
    val CountryIsUK = QuantitytyMorethan1000DF.where("Country = 'United Kingdom'")

    //If you like to understandd execution plan for both Logical And Physical plan, please uncomment the below line

    CountryIsUK.explain(extended = true)

//    ReaderBuilder

    //Action
    val UKSalesCount=CountryIsUK.count()

    println("Transformed Count is " + UKSalesCount)

    println(UKSalesCount)

    spark.wait()

  }

}
