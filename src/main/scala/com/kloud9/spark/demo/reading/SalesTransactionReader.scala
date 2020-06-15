package com.kloud9.spark.demo.reading

import java.time.{Duration, ZoneOffset, ZonedDateTime}

import com.kloud9.spark.demo.utilities.ReaderBuilder
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

import scala.reflect.io.Path

class SalesTransactionReader extends ReaderBuilder {

  def read(dateOfFeed: ZonedDateTime, Hour : Duration): DataFrame = {

    val SalesTransactionSchema: StructType = ???

    val salesTransactionBasePath = Path("/Users/nithya/work/gitrepos/spark_batch_processing/src/main/scala/com/kloud9/spark/demo/data/raw/")

    val startDate: ZonedDateTime = ZonedDateTime.now(ZoneOffset.UTC)

    val halfDay: Duration = Duration.ofHours(1)

    val salesTransactionReader = new ReaderBuilder()
      .withFormat("csv")
      .withSchema(SalesTransactionSchema)
      .withHourlyPathBuilder(salesTransactionBasePath,startDate,halfDay)
      .buildReader()

    val salesDF: DataFrame = salesTransactionReader.read()
    return salesDF
  }


}
