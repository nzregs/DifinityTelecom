// Databricks notebook source
// MAGIC %md
// MAGIC # Difinity Telecom - Cold Path Processing
// MAGIC 
// MAGIC ## Event Hub Capture (AVRO on Azure Blob Storage) >> Azure Databricks >> Data Lake Gen2 (parquet) >> SQL Data Warehouse
// MAGIC 
// MAGIC In the cold path, we process the event data that has been automatically captured to AVRO files in Azure Blob Storage, persist it to Parquet files on Azure Datalake Gen2 indefinately, and use the Parquet files to load into SQL Data Warehouse 

// COMMAND ----------

// MAGIC %md
// MAGIC #### Configure a parameter for this notebook so we know which time window to process

// COMMAND ----------

dbutils.widgets.text("window", "2019-02-01T00:00:00Z","Time to process yyyy-MM-dd-hh")

// COMMAND ----------

// MAGIC %md
// MAGIC #### Read AVRO file from Event Hub Capture location

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 1. Mount the blob storage directory

// COMMAND ----------

// mount file location
//dbutils.fs.mount(
//  source = "wasbs://ehcaptures@difinitytel.blob.core.windows.net/",
//  mountPoint = "/mnt/ehcaptures",
//  extraConfigs = Map("fs.azure.account.key.difinitytel.blob.core.windows.net" -> dbutils.secrets.get(scope = "DifinityTelecomKV", key = "DifinityTel-Storage-difinitytel")))


// COMMAND ----------

// MAGIC %md
// MAGIC ##### 2. Construct the filepath from the passed parameter

// COMMAND ----------

import java.time._

// param format: 2018-08-01T00:00:00Z
// need to replace Z because parser breaks with it in
val window = dbutils.widgets.get("window");

val instantdatetime = Instant.parse(window)
val windowdatetime = LocalDateTime.ofInstant(instantdatetime, ZoneOffset.UTC)
val windowyear = windowdatetime.getYear()
val windowmonth =windowdatetime.getMonthValue()
val windowday = windowdatetime.getDayOfMonth()
val windowhour =windowdatetime.getHour()

// create filename pattern based on parameter

val filepath = "/mnt/ehcaptures/" + windowyear + "/" + "%02d".format(windowmonth) +  "/" + "%02d".format(windowday) + "/" + "%02d".format(windowhour) + "/difinitytel/towerfeed/*/*/*.avro"

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 3. Load the AVRO file data into a dataframe

// COMMAND ----------

val avroDF = spark.read.format("avro")
  .load(filepath)

// COMMAND ----------

// MAGIC %md
// MAGIC ##### 4. Create a class to read event record into 

// COMMAND ----------

import java.sql.Timestamp

case class cdrSchema(
  towerId: String,
  eventDate: Timestamp,
  imei: String, 
  fromNumber: String,
  toNumber: String,
  billingType: String,
  duration: Int,
  bytes: Integer,
  protocol: Integer,
  port: Integer,
  uri: String,
  cost: Double 
)

// COMMAND ----------

// MAGIC %md
// MAGIC #### 5. Transform the AVRO dataframe (payload in "body" column) into columns using JSON transform 

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql._

val cdrDF = avroDF.select($"body" cast "string" as "json")
      .select(from_json($"json", Encoders.product[cdrSchema].schema) as "record")
      .selectExpr("record.*")

// COMMAND ----------

//quick check to ensure data looks correct
display(cdrDF)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Load in reference files to enrich the data

// COMMAND ----------

val towersDF = sqlContext.read.option("multiline", "true").json("/mnt/reference/towers.json")
val websitesDF = sqlContext.read.option("multiline", "true").json("/mnt/reference/websites.json")
val subscribersDF = sqlContext.read.option("multiline", "true").json("/mnt/reference/subscribers.json")
val dataspecialsDF = sqlContext.read.option("multiline", "true").json("/mnt/reference/dataspecials.json")

// COMMAND ----------

// MAGIC %md
// MAGIC ### Offer dataframes as SQL Temp Tables

// COMMAND ----------

towersDF.createOrReplaceTempView("towers")
websitesDF.createOrReplaceTempView("websites")
subscribersDF.createOrReplaceTempView("subscribers")
dataspecialsDF.createOrReplaceTempView("dataspecials")
cdrDF.createOrReplaceTempView("cdrData")

// COMMAND ----------

// MAGIC %md
// MAGIC ### Use SQL to enrich, aggregate, the data

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE OR REPLACE TEMPORARY VIEW subscriberSummary
// MAGIC AS
// MAGIC SELECT 
// MAGIC   min(cdrData.eventdate) as EventDate, 
// MAGIC   cdrData.billingType as BillingType,
// MAGIC   cdrData.fromnumber as SubscriberNumber, 
// MAGIC   cdrData.tonumber as CalledNumber,  
// MAGIC   sum(cdrData.duration) as CallDuration,
// MAGIC   null as SMSCount,
// MAGIC   null as URI, 
// MAGIC   null as MB,
// MAGIC   null as CostPerMB,
// MAGIC   null as URICost,
// MAGIC   null as Dataplan,
// MAGIC   null as DataOffer  
// MAGIC FROM
// MAGIC   cdrData 
// MAGIC     LEFT OUTER JOIN subscribers ON cdrData.fromnumber = subscribers.SubscriberNumber
// MAGIC WHERE
// MAGIC   cdrData.billingType = 'call'
// MAGIC GROUP BY
// MAGIC   cdrData.fromnumber, 
// MAGIC   cdrData.tonumber,
// MAGIC   cdrData.billingType
// MAGIC UNION ALL
// MAGIC SELECT 
// MAGIC   min(cdrData.eventdate) as EventDate, 
// MAGIC   cdrData.billingType as BillingType,
// MAGIC   cdrData.fromnumber as SubscriberNumber, 
// MAGIC   cdrData.tonumber as CalledNumber,  
// MAGIC   null as CallDuration,
// MAGIC   count(cdrData.eventdate) as SMSCount,
// MAGIC   null as URI, 
// MAGIC   null as MB,
// MAGIC   null as CostPerMB,
// MAGIC   null as URICost,
// MAGIC   null as Dataplan,
// MAGIC   null as DataOffer  
// MAGIC FROM
// MAGIC   cdrData 
// MAGIC     LEFT OUTER JOIN subscribers ON cdrData.fromnumber = subscribers.SubscriberNumber
// MAGIC WHERE
// MAGIC   cdrData.billingType = 'sms'
// MAGIC GROUP BY
// MAGIC   cdrData.fromnumber, 
// MAGIC   cdrData.tonumber,
// MAGIC   cdrData.billingType
// MAGIC UNION ALL
// MAGIC SELECT 
// MAGIC   min(cdrData.eventdate) as EventDate,
// MAGIC   cdrData.billingType as BillingType,
// MAGIC   cdrData.fromnumber as SubscriberNumber,
// MAGIC   null as CalledNumber,
// MAGIC   null as CallDuration,
// MAGIC   null as SMSCount,
// MAGIC   cdrData.uri as URI, 
// MAGIC   sum(cdrData.bytes)/1024 as MB,
// MAGIC   sum(costpermb) as CostPerMB,
// MAGIC   sum(cdrData.bytes/1024) * min(costpermb) as URICost,
// MAGIC   "" as Dataplan,
// MAGIC   dataspecials.offer as DataOffer  
// MAGIC FROM
// MAGIC   cdrData 
// MAGIC     LEFT OUTER JOIN subscribers ON cdrData.fromnumber = subscribers.SubscriberNumber
// MAGIC     LEFT OUTER JOIN dataspecials ON cdrData.uri = dataspecials.uri
// MAGIC WHERE
// MAGIC   cdrData.billingType = 'data'
// MAGIC GROUP BY
// MAGIC   cdrData.fromnumber, 
// MAGIC   cdrData.uri,
// MAGIC   cdrData.billingType,
// MAGIC   dataspecials.offer

// COMMAND ----------

// MAGIC %sql
// MAGIC -- quick check to see that the data looks correct
// MAGIC SELECT * FROM SubscriberSummary

// COMMAND ----------

// MAGIC %md
// MAGIC ### Write enriched data out to parquet file in Azure Datalake

// COMMAND ----------

// bring SQL temporary table back as dataframe 
val subscriberSummaryDF = spark.sql("SELECT " + windowyear + " as Year, " + windowmonth + " as Month, " + windowday + " as Day, " + windowhour + " as Hour, * FROM subscriberSummary")

// COMMAND ----------

// write out, file per hout, using parquet for optimal file size and speed
val tmpParquetPath = "/mnt/hourly-subscriber-summary/temp/parquet/" + windowyear + "%02d".format(windowmonth) + "%02d".format(windowday) + "%02d".format(windowhour) 
val outputParquetFile = "/mnt/hourly-subscriber-summary/parquet/" + windowyear + "%02d".format(windowmonth) + "%02d".format(windowday) + "%02d".format(windowhour) + "_consolidated.parquet" 

subscriberSummaryDF.coalesce(1).write.mode("overwrite").parquet(tmpParquetPath)
val tmpParquetFile = dbutils.fs.ls(tmpParquetPath).filter(file=>file.name.endsWith(".parquet"))(0).path

dbutils.fs.cp(tmpParquetFile,outputParquetFile)
dbutils.fs.rm(tmpParquetFile)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Use Azure Data Factory to pick up the parquet file and process into SQL Data Warehouse