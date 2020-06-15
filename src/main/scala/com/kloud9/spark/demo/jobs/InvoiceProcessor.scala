package com.kloud9.spark.demo.jobs

import com.kloud9.spark.demo.context.{InvoiceProcessorContext, sparkSessionSingleton}
import com.kloud9.spark.demo.utilities.ReaderBuilder

object InvoiceProcessor extends App {

    val singletonSparkSession = new InvoiceProcessorContext()
    val spark = singletonSparkSession.getSparkSession()

    println("Spark App Name  : " + singletonSparkSession.jobName)
    println("Number of Parallelism for Spark job : " + singletonSparkSession.defaultParallelism)

    val salesDF = new ReaderBuilder().read()
    salesDF.show(100)

//  val df: DataFrame = SalesTransactionReader.read(startDate, halfDay)


  }
