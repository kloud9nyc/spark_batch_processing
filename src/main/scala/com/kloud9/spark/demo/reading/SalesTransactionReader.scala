package com.kloud9.spark.demo.reading

import java.time.{Duration, ZoneOffset, ZonedDateTime}

import com.kloud9.spark.demo.utilities.ReaderBuilder
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

import scala.reflect.io.Path

class SalesTransactionReader extends ReaderBuilder {

def readData(): DataFrame = {
    val SalesTransactionSchema: StructType = null
    val salesTransactionBasePath = Path("src/main/scala/com/kloud9/spark/demo/data/raw/")
    val startDate: ZonedDateTime = ZonedDateTime.now(ZoneOffset.UTC)
    val salesDF : DataFrame = new ReaderBuilder()
    .withFormat("csv")
    .withSchema(SalesTransactionSchema)
    .withHourlyPathBuilder(salesTransactionBasePath,startDate)
    .read()
    return salesDF
  }

}
