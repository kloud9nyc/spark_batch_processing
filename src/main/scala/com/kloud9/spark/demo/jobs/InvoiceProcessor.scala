package com.kloud9.spark.demo.jobs

import com.kloud9.spark.demo.context.InvoiceProcessorContext
import com.kloud9.spark.demo.utilities.ReaderBuilder
import com.kloud9.spark.demo.reading.SalesTransactionReader

object InvoiceProcessor extends App {
    try {
      val singletonSparkSession = new InvoiceProcessorContext()
      val spark = singletonSparkSession.getSparkSession()

      println("Spark App Name  : " + singletonSparkSession.jobName)
      println("Number of Parallelism for Spark job : " + singletonSparkSession.defaultParallelism)

      val df = new SalesTransactionReader().readData()
      df.show(100)
      if(df==null)
        {
          println("No Data Found Exception / Path Not Exists")

        }

    }
  catch
  {
    case e=> println(e)

  }
  }
