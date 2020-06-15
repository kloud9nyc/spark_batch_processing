package com.kloud9.spark.demo.jobs

import com.kloud9.spark.demo.context.sparkSessionSingleton
import com.kloud9.spark.demo.utilities.ReaderBuilder

object InvoiceProcessor extends App {

    val singletonSparkSession = new sparkSessionSingleton()
    val spark = singletonSparkSession.getSparkSession()

    println("Spark App Name  : " + singletonSparkSession.jobName)
    println("Number of Parallelism for Spark job : " + singletonSparkSession.defaultParallelism)

    val salesDF = new ReaderBuilder().read()
    myDF.show(100)

//  val df: DataFrame = SalesTransactionReader.read(startDate, halfDay)


  }
