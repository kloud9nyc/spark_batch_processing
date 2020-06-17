package com.kloud9.spark.demo.utilities

import java.time.format.DateTimeFormatter
import java.time.{Duration, ZoneOffset, ZonedDateTime}

import com.kloud9.spark.demo.context.InvoiceProcessorContext
import com.kloud9.spark.demo.jobs.InvoiceProcessor
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

import scala.reflect.io.Path


class ReaderBuilder{
  val singletonSparkSession = new InvoiceProcessorContext()
  val spark = singletonSparkSession.getSparkSession()
  protected var readSchema : StructType = null
  protected var readFormat : String = ""
  protected var filePath : String = ""

  def withFormat(fileFormat: String): this.type = {
    this.readFormat = fileFormat
    this
  }

  def withSchema(schema: StructType): this.type = {
    this.readSchema = schema
    this
  }
  def withHourlyPathBuilder(basePath: Path, startDate: ZonedDateTime): this.type = {
    try {
      val year = startDate.format(DateTimeFormatter.ofPattern("YYYY"))
      val date = startDate.format(DateTimeFormatter.ofPattern("dd"))
      val month = startDate.format(DateTimeFormatter.ofPattern("MM"))
      val hour = startDate.getHour()-1
      val pathDetails = basePath + "/" + year + "/" + month + "/" + date + "/" + hour
      this.filePath = pathDetails
    } catch {
      case e:Throwable => println(e)
    }
    this
  }
  def buildReader(): this.type = {
    this
  }

  def read(): DataFrame = {
    var df: DataFrame = null
    try {

      df = this.readFormat match {
        case "csv" =>
          spark.read.schema(this.readSchema).format(this.readFormat).option("header", "true").option("inferSchema", "true").load(this.filePath)
      }
    }
    catch {
      case e:Throwable => println(e)
        df=null

    }
    df
  }
}
