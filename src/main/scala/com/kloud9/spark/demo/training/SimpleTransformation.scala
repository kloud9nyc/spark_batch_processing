package com.kloud9.spark.demo.training

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SimpleTransformation {

  def main(args: Array[String]): Unit = {

    //Setup Logging
    Logger.getLogger("org").setLevel(Level.ERROR)

    //Constants
    val dataroute = "/Users/nithya/work/gitrepos/spark_batch_processing/src/main/scala/com/kloud9/spark/demo/data/raw/"

    //Setup Spark Session
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("SimpleTransformation")
      .getOrCreate()

    //Read File

    val salesRDD = spark.sparkContext.textFile("file:///"+ dataroute+"sales.csv")

    salesRDD.count()

    salesRDD.take(10).foreach(println)


    val salesDF = spark.read.format("csv").option("header","true").option("inferSchema","true").load("file:///"+ dataroute+"sales.csv")

    //Count of Entire Data Frame by Applying Count Action
    println(salesDF.count())
    salesDF.show(10)

//    Applying Transformation but Not Applied any Action"
    salesDF.where("CustomerId is Not Null")

//    println("Now Applying Count Action, But you will not see any changes in the Data Frame since it is Immutable"
    println(salesDF.count())

//    "Now Applying Transformation and Assigning to another variable"
    val CustomerNotNull = salesDF.where("CustomerId is Not Null")

//    "Now Applying Count Action on the New DataFrame, Now you can see the count is reduced"
    println(CustomerNotNull.count())

    println("============================Summary============")
    println("Spark Objects [DF/RDD] are immutable, If you apply any transformation, and action on it, it will always create new Object [DF/RDD]")
  }

}
