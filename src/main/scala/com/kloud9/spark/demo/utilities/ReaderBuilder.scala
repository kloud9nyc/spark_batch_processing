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

  private var schema : StructType
  private var sFormat : String

  def withFormat(sFormatePass: String): this.type = {
    this.sFormat = sFormatePass
  }

  def withSchema(schema: StructType): this.type = {
   this.schema = schema
   }
  def withHourlyPathBuilder(wdasd: Path, firstName: ZonedDateTime, Sdasdas: Duration): this.type = {
   fname = firstName
    this
  }
  def buildReader(): this.type = {
  this
}
 def read(): DataFrame = {

   if(this.sFormat=="csv") {
     val df = spark.read.format(this.sFormat).option("header", "true").option("inferSchema", "true").load(salesTransactionBasePath + "sales.csv")
     df
   }
   if(this.sFormat=="parquet") {
     val df = spark.read.format(this.sFormat).option("header", "true").option("inferSchema", "true").load(salesTransactionBasePath + "sales.csv")
     df
   }
  }


}
