package com.kloud9.spark.demo.context

import java.util.Properties

import org.apache.log4j.{Level, Logger, PropertyConfigurator}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

class InvoiceProcessorContext {

  /** Set the Log Level */
  Logger.getLogger("org").setLevel(Level.ERROR)

  /** Read the context properties files based on the Environment */
  val connectionParam = new Properties
  connectionParam.load(getClass().getResourceAsStream("../config/invoiceProcessor.context.properties"))
  PropertyConfigurator.configure(connectionParam)

  /** Get the required details from properties file to avoid the hardcoding value inside code */

  val jobName = connectionParam.getProperty("jobName")
  val driveCore = connectionParam.getProperty("driverCore")
  val defaultParallelism = connectionParam.getProperty("defaultParallelism")

  @transient private var sparkSession: SparkSession = null
  /** Creating SparkSession */
  def getSparkSession(): SparkSession = {
    val conf = new SparkConf()
      .setAppName(jobName)
      .set("spark.default.parallelism", defaultParallelism)
      .setIfMissing("spark.master", "local[*]")
      .set("spark.hadoop.fs.s3a.multiobjectdelete.enable","false")
      .set("spark.hadoop.fs.s3a.fast.upload","true")
      .set("spark.sql.parquet.filterPushdown", "true")
      .set("spark.sql.parquet.mergeSchema", "false")
      .set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
      .set("spark.speculation", "false")
      .set("spark.driver.cores",driveCore)

    /** Checkpointing can be enabled wherever applicable */

    /** Individual Spark Session Context should be created to avoid the checkpoint issues  */
    if (sparkSession == null) {
      sparkSession = SparkSession.builder().config(conf).getOrCreate()
    }
    sparkSession  }

}
