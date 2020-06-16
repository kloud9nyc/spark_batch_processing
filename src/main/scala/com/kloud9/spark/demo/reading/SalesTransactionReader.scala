package com.kloud9.spark.demo.reading

import java.time.{Duration, ZoneOffset, ZonedDateTime}
import java.util.Properties

import com.kloud9.spark.demo.utilities.ReaderBuilder
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import com.kloud9.spark.demo.schemas.invoiceProcessor.SalesTransactionSchema
import org.apache.log4j.PropertyConfigurator

import scala.reflect.io.Path

class SalesTransactionReader extends ReaderBuilder with SalesTransactionSchema {

    /** Read the context properties files based on the Environment */
    val appConfiguration = new Properties
    appConfiguration.load(getClass().getResourceAsStream("../config/invoiceProcessor.context.properties"))
    PropertyConfigurator.configure(appConfiguration)

def readData(): DataFrame = {
    val salesTransactionBasePath = appConfiguration.getProperty("salesTransactionFeedPath")
    val startDate: ZonedDateTime = ZonedDateTime.now(ZoneOffset.UTC)
    val salesDF : DataFrame = new ReaderBuilder()
    .withFormat("csv")
    .withSchema(salesTransactionSchema)
    .withHourlyPathBuilder(salesTransactionBasePath,startDate)
    .read()
    return salesDF
  }

}
