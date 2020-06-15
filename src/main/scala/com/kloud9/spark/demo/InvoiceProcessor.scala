package com.kloud9.spark.demo

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import com.kloud9.spark.demo.context.sparkSessionSingleton
import com.kloud9.spark.demo.reading.SalesTransactionReader
import com.kloud9.spark.demo.utilities.ReaderBuilder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.reflect.io.Path

object InvoiceProcessor extends App {

    val singletonSparkSession = new sparkSessionSingleton()
    val spark = singletonSparkSession.getSparkSession()

    println("Spark App Name  : " + singletonSparkSession.jobName)
    println("Number of Parallelism for Spark job : " + singletonSparkSession.defaultParallelism)

    val salesDF = new ReaderBuilder().read()
    myDF.show(100)

//  val df: DataFrame = UserBehaviourReader.read(startDate, halfDay)


  }
