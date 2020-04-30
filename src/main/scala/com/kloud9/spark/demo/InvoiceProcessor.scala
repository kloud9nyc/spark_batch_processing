package com.kloud9.spark.demo

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object InvoiceProcessor {

  def main(args: Array[String]): Unit = {

    //Setup Logging
    Logger.getLogger("org").setLevel(Level.ERROR)

    //Setup Spark Session
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("InvoiceProcessor")
      .config("spark.hadoop.fs.s3a.multiobjectdelete.enable","false")
      .config("spark.hadoop.fs.s3a.fast.upload","true")
      .config("spark.sql.parquet.filterPushdown", "true")
      .config("spark.sql.parquet.mergeSchema", "false")
      .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
      .config("spark.speculation", "false")
      .getOrCreate()



   //Preparing S3 Work Path

    val year = LocalDate.now().format(DateTimeFormatter.ofPattern("YYYY"))

    val date = LocalDate.now().format(DateTimeFormatter.ofPattern("dd"))


    val month = LocalDate.now().format(DateTimeFormatter.ofPattern("MM"))

    val fileName="sales.csv"
    val rawFolderName ="raw"
    val processedFolderName ="processed"
    val s3BucketName = "data-engineering-k9"

    val inputPath = s3BucketName +"/" + year+"/" +month +"/" +date +"/" + rawFolderName +"/" + fileName
    val outputPath = s3BucketName +"/" + year+"/" +month +"/" +date +"/" + processedFolderName +"/output"

    val s3InputUrl = "s3a://"+inputPath
    val s3OutputUrl = "s3a://"+outputPath

    println(s3InputUrl)

    println(s3OutputUrl)


    //AWS S3 Configuration - Credentials
    spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.access.key", "AKIATIZYOWFFUPUCP35I")
    // Replace Key with your AWS secret key (You can find this on IAM
    spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.secret.key", "abo6D4CND8HPp1xrW4wbritvg8f86mJ8JAlxblkU")

    spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.endpoint", "s3.us-west-2.amazonaws.com")

    val df = spark.read.format("csv").option("header","true").option("inferSchema","true").load(s3InputUrl)

    df.createOrReplaceTempView("data")

    val res = spark.sql("select * from data where UnitPrice > 3 ")

    res.show(10)
    println(res.count())

    res.write.format("csv").option("header","true").mode("Overwrite").save(s3OutputUrl)

    println("Process Completed")


  }

}
