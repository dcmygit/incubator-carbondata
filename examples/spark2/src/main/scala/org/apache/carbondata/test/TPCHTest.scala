/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.examples

import java.io.File

import org.apache.spark.sql.SparkSession

import org.apache.carbondata.core.constants.{CarbonCommonConstants, CarbonV3DataFormatConstants}
import org.apache.carbondata.core.util.CarbonProperties

// scalastyle:off println
object TPCHTest {

  val blockletSize = "128"
  val blockSize = "300"

  def main(args: Array[String]): Unit = {
    val rootPath = new File(this.getClass.getResource("/").getPath
                            + "../../../..").getCanonicalPath
    val storeLocation = s"$rootPath/examples/spark2/target/store"
    val warehouse = s"$rootPath/examples/spark2/target/warehouse"
    val metastoredb = s"$rootPath/examples/spark2/target"

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy-MM-dd HH:mm:ss")
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy-MM-dd")
      .addProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE_LOADING, "true")
      .addProperty(CarbonV3DataFormatConstants.BLOCKLET_SIZE_IN_MB, blockletSize)

    import org.apache.spark.sql.CarbonSession._
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("CarbonSessionExample")
      .enableHiveSupport()
      .config("spark.sql.warehouse.dir", warehouse)
      .config("spark.driver.host", "localhost")
      .getOrCreateCarbonSession(storeLocation, metastoredb)

    spark.sparkContext.setLogLevel("WARN")

    val source = s"$rootPath/../lineitem_csv"
    println("carbon")
//    loadCarbon(source, spark)
//    testCarbon(spark)
    loadCarbonBigInt(source, spark)
    loadCarbonDecimal(source, spark)
    loadCarbonDate(source, spark)
    loadCarbonShortString(source, spark)
    loadCarbonLongString(source, spark)

    println("parquet")
    prepareCSVTable(source, spark)
//    loadParquet(source, spark)
//    testParquet(spark)
    loadParquetBigInt(source, spark)
    loadParquetDecimal(source, spark)
    loadParquetDate(source, spark)
    loadParquetShortString(source, spark)
    loadParquetLongString(source, spark)

    println("orc")
    loadOrcBigInt(source, spark)
    loadOrcDecimal(source, spark)
    loadOrcDate(source, spark)
    loadOrcShortString(source, spark)
    loadOrcLongString(source, spark)

    // Drop table
    //spark.sql("DROP TABLE IF EXISTS LINEITEM_carbon")
    //spark.sql("DROP TABLE IF EXISTS LINEITEM_parquet")

    spark.stop()
  }

  private def loadOrcBigInt(sourceDataPath: String, spark: SparkSession): Unit = {
    println("loadOrcBigInt")
    spark.sql("DROP TABLE IF EXISTS LINEITEM_orc_bigint")
    spark.sql(
      s"""
         | CREATE TABLE LINEITEM_orc_bigint (
         |	L_ORDERKEY	BIGINT,
         |	L_PARTKEY		BIGINT,
         |	L_SUPPKEY		BIGINT
         | )
         | USING ORC
       """.stripMargin)
    val start = System.nanoTime()
    spark.sql(
      s"""
         | INSERT OVERWRITE TABLE LINEITEM_orc_bigint
         | SELECT L_ORDERKEY, L_PARTKEY, L_SUPPKEY FROM LINEITEM_csv
       """.stripMargin)

    val ms = (System.nanoTime() - start) / 1000 / 1000
    println(s"loading time: $ms ms")
  }

  private def loadParquetBigInt(sourceDataPath: String, spark: SparkSession): Unit = {
    println("loadParquetBigInt")
    spark.sql("DROP TABLE IF EXISTS LINEITEM_parquet_bigint")
    spark.sql(
      s"""
         | CREATE TABLE LINEITEM_parquet_bigint (
         |	L_ORDERKEY	BIGINT,
         |	L_PARTKEY		BIGINT,
         |	L_SUPPKEY		BIGINT
         | )
         | USING parquet
       """.stripMargin)
    val start = System.nanoTime()
    spark.sql(
      s"""
         | INSERT OVERWRITE TABLE LINEITEM_parquet_bigint
         | SELECT L_ORDERKEY, L_PARTKEY, L_SUPPKEY FROM LINEITEM_csv
       """.stripMargin)

    val ms = (System.nanoTime() - start) / 1000 / 1000
    println(s"loading time: $ms ms")
  }

  private def loadCarbonBigInt(sourceDataPath: String, spark: SparkSession): Unit = {
    println("loadCarbonBigInt")
    spark.sql("DROP TABLE IF EXISTS LINEITEM_carbon_bigint")
    spark.sql(
      s"""
         | CREATE TABLE LINEITEM_carbon_bigint (
         |	L_ORDERKEY	BIGINT,
         |	L_PARTKEY		BIGINT,
         |	L_SUPPKEY		BIGINT
         | )
         | STORED BY 'carbondata'
         | TBLPROPERTIES("SORT_COLUMNS"="", 'table_blocksize'='$blockSize')
       """.stripMargin)
    val start = System.nanoTime()
    spark.sql(
      s"""
         | LOAD DATA LOCAL INPATH '$sourceDataPath/lineitem'
         | INTO TABLE LINEITEM_carbon_bigint
         | OPTIONS ('delimiter'='|', 'header'='true', 'sort_scope'='no_sort')
       """.stripMargin)

    val ms = (System.nanoTime() - start) / 1000 / 1000
    println(s"loading time: $ms ms")
  }

  private def loadOrcDecimal(sourceDataPath: String, spark: SparkSession): Unit = {
    println("loadOrcDecimal")
    spark.sql("DROP TABLE IF EXISTS LINEITEM_orc_decimal")
    spark.sql(
      s"""
         | CREATE TABLE LINEITEM_orc_decimal (
         |	L_QUANTITY		DECIMAL,
         |	L_EXTENDEDPRICE	DECIMAL,
         |	L_DISCOUNT		DECIMAL,
         |	L_TAX			DECIMAL
         | )
         | USING ORC
       """.stripMargin)
    val start = System.nanoTime()
    spark.sql(
      s"""
         | INSERT OVERWRITE TABLE LINEITEM_orc_decimal
         | SELECT L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX FROM LINEITEM_csv
       """.stripMargin)

    val ms = (System.nanoTime() - start) / 1000 / 1000
    println(s"loading time: $ms ms")
  }

  private def loadParquetDecimal(sourceDataPath: String, spark: SparkSession): Unit = {
    println("loadParquetDecimal")
    spark.sql("DROP TABLE IF EXISTS LINEITEM_parquet_decimal")
    spark.sql(
      s"""
         | CREATE TABLE LINEITEM_parquet_decimal (
         |	L_QUANTITY		DECIMAL,
         |	L_EXTENDEDPRICE	DECIMAL,
         |	L_DISCOUNT		DECIMAL,
         |	L_TAX			DECIMAL
         | )
         | USING parquet
       """.stripMargin)
    val start = System.nanoTime()
    spark.sql(
      s"""
         | INSERT OVERWRITE TABLE LINEITEM_parquet_decimal
         | SELECT L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX FROM LINEITEM_csv
       """.stripMargin)

    val ms = (System.nanoTime() - start) / 1000 / 1000
    println(s"loading time: $ms ms")
  }

  private def loadCarbonDecimal(sourceDataPath: String, spark: SparkSession): Unit = {
    println("loadCarbonDecimal")
    spark.sql("DROP TABLE IF EXISTS LINEITEM_carbon_decimal")
    spark.sql(
      s"""
         | CREATE TABLE LINEITEM_carbon_decimal (
         |	L_QUANTITY		DECIMAL,
         |	L_EXTENDEDPRICE	DECIMAL,
         |	L_DISCOUNT		DECIMAL,
         |	L_TAX			DECIMAL
         | )
         | STORED BY 'carbondata'
         | TBLPROPERTIES("SORT_COLUMNS"="", 'table_blocksize'='$blockSize')
       """.stripMargin)
    val start = System.nanoTime()
    spark.sql(
      s"""
         | LOAD DATA LOCAL INPATH '$sourceDataPath/lineitem'
         | INTO TABLE LINEITEM_carbon_decimal
         | OPTIONS ('delimiter'='|', 'header'='true', 'sort_scope'='no_sort')
       """.stripMargin)

    val ms = (System.nanoTime() - start) / 1000 / 1000
    println(s"loading time: $ms ms")
  }

  private def loadOrcDate(sourceDataPath: String, spark: SparkSession): Unit = {
    println("loadOrcDate")
    spark.sql("DROP TABLE IF EXISTS LINEITEM_orc_date")

    // Create table
    spark.sql(
      s"""
         | CREATE TABLE LINEITEM_orc_date (
         |	L_SHIPDATE		DATE,
         |	L_COMMITDATE	DATE,
         |	L_RECEIPTDATE	DATE
         | )
         | USING ORC
       """.stripMargin)

    val start = System.nanoTime()
    spark.sql(
      s"""
         | INSERT OVERWRITE TABLE LINEITEM_orc_date
         | SELECT L_SHIPDATE, L_COMMITDATE, L_RECEIPTDATE FROM LINEITEM_csv
       """.stripMargin)

    val ms = (System.nanoTime() - start) / 1000 / 1000
    println(s"loading time: $ms ms")
  }

  private def loadParquetDate(sourceDataPath: String, spark: SparkSession): Unit = {
    println("loadParquetDate")
    spark.sql("DROP TABLE IF EXISTS LINEITEM_parquet_date")

    // Create table
    spark.sql(
      s"""
         | CREATE TABLE LINEITEM_parquet_date (
         |	L_SHIPDATE		DATE,
         |	L_COMMITDATE	DATE,
         |	L_RECEIPTDATE	DATE
         | )
         | USING parquet
       """.stripMargin)

    val start = System.nanoTime()
    spark.sql(
      s"""
         | INSERT OVERWRITE TABLE LINEITEM_parquet_date
         | SELECT L_SHIPDATE, L_COMMITDATE, L_RECEIPTDATE FROM LINEITEM_csv
       """.stripMargin)

    val ms = (System.nanoTime() - start) / 1000 / 1000
    println(s"loading time: $ms ms")
  }

  private def loadCarbonDate(sourceDataPath: String, spark: SparkSession): Unit = {
    println("loadCarbonDate")
    spark.sql("DROP TABLE IF EXISTS LINEITEM_carbon_date")

    // Create table
    spark.sql(
      s"""
         | CREATE TABLE LINEITEM_carbon_date (
         |	L_SHIPDATE		DATE,
         |	L_COMMITDATE	DATE,
         |	L_RECEIPTDATE	DATE
         | )
         | STORED BY 'carbondata'
         | TBLPROPERTIES("SORT_COLUMNS"="", 'table_blocksize'='$blockSize')
       """.stripMargin)

    val start = System.nanoTime()
    spark.sql(
      s"""
         | LOAD DATA LOCAL INPATH '$sourceDataPath/lineitem'
         | INTO TABLE LINEITEM_carbon_date
         | OPTIONS ('delimiter'='|', 'header'='true', 'sort_scope'='no_sort')
       """.stripMargin)

    val ms = (System.nanoTime() - start) / 1000 / 1000
    println(s"loading time: $ms ms")
  }

  private def loadOrcShortString(sourceDataPath: String, spark: SparkSession): Unit = {
    println("loadOrcShortString")
    spark.sql("DROP TABLE IF EXISTS LINEITEM_orc_short_string")

    // Create table
    spark.sql(
      s"""
         | CREATE TABLE LINEITEM_orc_short_string (
         |	L_SHIPINSTRUCT	CHAR(25),
         |	L_SHIPMODE		CHAR(10)
         | )
         | USING ORC
       """.stripMargin)

    val start = System.nanoTime()
    spark.sql(
      s"""
         | INSERT OVERWRITE TABLE LINEITEM_orc_short_string
         | SELECT L_SHIPINSTRUCT, L_SHIPMODE FROM LINEITEM_csv
       """.stripMargin)

    val ms = (System.nanoTime() - start) / 1000 / 1000
    println(s"loading time: $ms ms")
  }

  private def loadOrcLongString(sourceDataPath: String, spark: SparkSession): Unit = {
    println("loadOrcLongString")
    spark.sql("DROP TABLE IF EXISTS LINEITEM_orc_long_string")

    // Create table
    spark.sql(
      s"""
         | CREATE TABLE LINEITEM_orc_long_string (
         |	L_COMMENT		VARCHAR(44)
         | )
         | USING ORC
       """.stripMargin)

    val start = System.nanoTime()
    spark.sql(
      s"""
         | INSERT OVERWRITE TABLE LINEITEM_orc_long_string
         | SELECT L_COMMENT FROM LINEITEM_csv
       """.stripMargin)

    val ms = (System.nanoTime() - start) / 1000 / 1000
    println(s"loading time: $ms ms")
  }

  private def loadParquetShortString(sourceDataPath: String, spark: SparkSession): Unit = {
    println("loadParquetShortString")
    spark.sql("DROP TABLE IF EXISTS LINEITEM_parquet_short_string")

    // Create table
    spark.sql(
      s"""
         | CREATE TABLE LINEITEM_parquet_short_string (
         |	L_SHIPINSTRUCT	CHAR(25),
         |	L_SHIPMODE		CHAR(10)
         | )
         | USING parquet
       """.stripMargin)

    val start = System.nanoTime()
    spark.sql(
      s"""
         | INSERT OVERWRITE TABLE LINEITEM_parquet_short_string
         | SELECT L_SHIPINSTRUCT, L_SHIPMODE FROM LINEITEM_csv
       """.stripMargin)

    val ms = (System.nanoTime() - start) / 1000 / 1000
    println(s"loading time: $ms ms")
  }

  private def loadParquetLongString(sourceDataPath: String, spark: SparkSession): Unit = {
    println("loadParquetLongString")
    spark.sql("DROP TABLE IF EXISTS LINEITEM_parquet_long_string")

    // Create table
    spark.sql(
      s"""
         | CREATE TABLE LINEITEM_parquet_long_string (
         |	L_COMMENT		VARCHAR(44)
         | )
         | USING parquet
       """.stripMargin)

    val start = System.nanoTime()
    spark.sql(
      s"""
         | INSERT OVERWRITE TABLE LINEITEM_parquet_long_string
         | SELECT L_COMMENT FROM LINEITEM_csv
       """.stripMargin)

    val ms = (System.nanoTime() - start) / 1000 / 1000
    println(s"loading time: $ms ms")
  }

  private def loadCarbonShortString(sourceDataPath: String, spark: SparkSession): Unit = {
    println("loadCarbonShortString")
    spark.sql("DROP TABLE IF EXISTS LINEITEM_carbon_short_string")

    // Create table
    spark.sql(
      s"""
         | CREATE TABLE LINEITEM_carbon_short_string (
         |	L_SHIPINSTRUCT	CHAR(25),
         |	L_SHIPMODE		CHAR(10)
         | )
         | STORED BY 'carbondata'
         | TBLPROPERTIES("SORT_COLUMNS"="L_SHIPINSTRUCT,L_SHIPMODE", 'table_blocksize'='$blockSize',
         | 'DICTIONARY_INCLUDE'='L_SHIPINSTRUCT,L_SHIPMODE')
       """.stripMargin)

    val start = System.nanoTime()
    spark.sql(
      s"""
         | LOAD DATA LOCAL INPATH '$sourceDataPath/lineitem'
         | INTO TABLE LINEITEM_carbon_short_string
         | OPTIONS ('delimiter'='|', 'header'='true')
       """.stripMargin)

    val ms = (System.nanoTime() - start) / 1000 / 1000
    println(s"loading time: $ms ms")
  }

  private def loadCarbonLongString(sourceDataPath: String, spark: SparkSession): Unit = {
    println("loadCarbonLongString")
    spark.sql("DROP TABLE IF EXISTS LINEITEM_carbon_long_string")

    // Create table
    spark.sql(
      s"""
         | CREATE TABLE LINEITEM_carbon_long_string (
         |	L_COMMENT		VARCHAR(44)
         | )
         | STORED BY 'carbondata'
         | TBLPROPERTIES("SORT_COLUMNS"="", 'table_blocksize'='$blockSize')
       """.stripMargin)

    val start = System.nanoTime()
    spark.sql(
      s"""
         | LOAD DATA LOCAL INPATH '$sourceDataPath/lineitem'
         | INTO TABLE LINEITEM_carbon_long_string
         | OPTIONS ('delimiter'='|', 'header'='true', 'sort_scope'='no_sort')
       """.stripMargin)

    val ms = (System.nanoTime() - start) / 1000 / 1000
    println(s"loading time: $ms ms")
  }

  private def prepareCSVTable(sourceDataPath: String,
      spark: SparkSession) = {
    spark.sql("DROP TABLE IF EXISTS LINEITEM_csv")
    spark.sql(
      s"""
         | CREATE EXTERNAL TABLE IF NOT EXISTS LINEITEM_csv (
         |	L_ORDERKEY	BIGINT,
         |	L_PARTKEY		BIGINT,
         |	L_SUPPKEY		BIGINT,
         |	L_LINENUMBER	INTEGER,
         |	L_QUANTITY		DECIMAL,
         |	L_EXTENDEDPRICE	DECIMAL,
         |	L_DISCOUNT		DECIMAL,
         |	L_TAX			DECIMAL,
         |	L_RETURNFLAG	CHAR(1),
         |	L_LINESTATUS	CHAR(1),
         |	L_SHIPDATE		DATE,
         |	L_COMMITDATE	DATE,
         |	L_RECEIPTDATE	DATE,
         |	L_SHIPINSTRUCT	CHAR(25),
         |	L_SHIPMODE		CHAR(10),
         |	L_COMMENT		VARCHAR(44)
         | )
         | ROW FORMAT DELIMITED
         | FIELDS TERMINATED BY '|'
         | STORED AS TEXTFILE
         | LOCATION '$sourceDataPath'
         | TBLPROPERTIES ("skip.header.line.count"="1")
       """.stripMargin)
  }

  private def loadParquet(sourceDataPath: String, spark: SparkSession): Unit = {
    spark.sql("DROP TABLE IF EXISTS LINEITEM_parquet")

    // Create table
    spark.sql(
      s"""
         | CREATE TABLE LINEITEM_parquet (
         |	L_ORDERKEY	BIGINT,
         |	L_PARTKEY		BIGINT,
         |	L_SUPPKEY		BIGINT,
         |	L_LINENUMBER	INTEGER,
         |	L_QUANTITY		DECIMAL,
         |	L_EXTENDEDPRICE	DECIMAL,
         |	L_DISCOUNT		DECIMAL,
         |	L_TAX			DECIMAL,
         |	L_RETURNFLAG	CHAR(1),
         |	L_LINESTATUS	CHAR(1),
         |	L_SHIPDATE		DATE,
         |	L_COMMITDATE	DATE,
         |	L_RECEIPTDATE	DATE,
         |	L_SHIPINSTRUCT	CHAR(25),
         |	L_SHIPMODE		CHAR(10),
         |	L_COMMENT		VARCHAR(44)
         | )
         | USING parquet
       """.stripMargin)

    val start = System.nanoTime()
    spark.sql(
      s"""
         | INSERT OVERWRITE TABLE LINEITEM_parquet
         | SELECT * FROM LINEITEM_csv
       """.stripMargin)

    val ms = (System.nanoTime() - start) / 1000 / 1000
    println(s"loading time: $ms ms")
  }

  private def testParquet(spark: SparkSession): Unit = {
    spark.sql(
      """
        |SELECT count(*)
        |FROM LINEITEM_parquet
      """.stripMargin).show()
    spark.sql(
      """
        |SELECT *
        |FROM LINEITEM_parquet limit 10
      """.stripMargin).show()
  }

  private def loadCarbon(sourceDataPath: String, spark: SparkSession): Unit = {
    spark.sql("DROP TABLE IF EXISTS LINEITEM_carbon")

    // Create table
    spark.sql(
      s"""
         | CREATE TABLE LINEITEM_carbon (
         |	L_ORDERKEY	BIGINT,
         |	L_PARTKEY		BIGINT,
         |	L_SUPPKEY		BIGINT,
         |	L_LINENUMBER	INTEGER,
         |	L_QUANTITY		DECIMAL,
         |	L_EXTENDEDPRICE	DECIMAL,
         |	L_DISCOUNT		DECIMAL,
         |	L_TAX			DECIMAL,
         |	L_RETURNFLAG	CHAR(1),
         |	L_LINESTATUS	CHAR(1),
         |	L_SHIPDATE		DATE,
         |	L_COMMITDATE	DATE,
         |	L_RECEIPTDATE	DATE,
         |	L_SHIPINSTRUCT	CHAR(25),
         |	L_SHIPMODE		CHAR(10),
         |	L_COMMENT		VARCHAR(44)
         | )
         | STORED BY 'carbondata'
         | TBLPROPERTIES("SORT_COLUMNS"="")
       """.stripMargin)

    val start = System.nanoTime()
    spark.sql(
      s"""
         | LOAD DATA LOCAL INPATH '$sourceDataPath/lineitem'
         | INTO TABLE LINEITEM_carbon
         | OPTIONS ('delimiter'='|', 'header'='true', 'sort_scope'='no_sort')
       """.stripMargin)

    val ms = (System.nanoTime() - start) / 1000 / 1000
    println(s"loading time: $ms ms")
  }

  private def testCarbon(spark: SparkSession): Unit = {
    spark.sql(
      """
        |SELECT count(*)
        |FROM LINEITEM_carbon
      """.stripMargin).show()
    spark.sql(
      """
        |SELECT *
        |FROM LINEITEM_carbon limit 10
      """.stripMargin).show()
  }
}

// scalastyle:on println