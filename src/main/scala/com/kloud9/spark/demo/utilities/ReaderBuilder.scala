package com.kloud9.spark.demo.utilities

import java.time.{Duration, ZoneOffset, ZonedDateTime}

import com.kloud9.spark.demo.jobs.InvoiceProcessor.spark
import com.kloud9.spark.demo.context.sparkSessionSingleton
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

import scala.reflect.io.Path


class ReaderBuilder{
  val salesTransactionBasePath = "/Users/nithya/work/gitrepos/spark_batch_processing/src/main/scala/com/kloud9/spark/demo/data/raw/"
  val singletonSparkSession = new sparkSessionSingleton()
  val spark = singletonSparkSession.getSparkSession()
  protected var readSchema : StructType = null
  protected var readFormat : String = ""

  def withFormat(sFormatePass: String): this.type = {
    this.readFormat = sFormatePass
    this
  }

  def withSchema(schema: StructType): this.type = {
    this.readSchema = schema
    this
  }
  def withHourlyPathBuilder(wdasd: Path, firstName: ZonedDateTime, Sdasdas: Duration): this.type = {
    //fname = firstName
    this
  }
  def buildReader(): this.type = {
    this
  }
  def read(): DataFrame = {
    val df = this.readFormat match {
    case "csv"  => spark.read.format(this.readFormat).option("header", "true").option("inferSchema", "true").load(salesTransactionBasePath + "sales.csv")
    case "parquet"  => spark.read.format(this.readFormat).option("header", "true").option("inferSchema", "true").load(salesTransactionBasePath + "sales.csv")
    //case _  => "Invalid Dataframe"  // the default, catch-all
    }
    df
  }
}
