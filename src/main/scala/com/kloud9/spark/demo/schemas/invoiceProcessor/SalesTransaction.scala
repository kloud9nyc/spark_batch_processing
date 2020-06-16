package com.kloud9.spark.demo.schemas.invoiceProcessor
import org.apache.spark.sql.types._

/** Sales Transaction Schema */
trait SalesTransactionSchema {

  val salesTransactionSchema = new StructType()
    .add("INVOICE_NO", StringType)
    .add("STOCK_CODE", IntegerType)
    .add("DESCRIPTION", StringType)
    .add("QUANTITY", StringType)
    .add("INVOICE_DATE", StringType)
    .add("UNIT_PRICE", StringType)
    .add("CUSTOMER_ID", StringType)
    .add("COUNTRY", StringType)
}
