// Databricks notebook source
// MAGIC %md
// MAGIC # Difinity Telecom - Warm Path Processing
// MAGIC 
// MAGIC ## Event Hubs >> Azure Databricks >> Cosmos DB
// MAGIC 
// MAGIC In the warm path, a windowed near-realtime stream from Azure Event Hubs is enriched, aggregated (by window), and pushed to Power BI using the Streaming Dataset API

// COMMAND ----------

// MAGIC %md
// MAGIC ### Create a java class for the Call Detail Record

// COMMAND ----------

import java.sql.Timestamp

case class CallDetailRecord(
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
// MAGIC ### Build an Event Hub connection string using the secrets from Azure Key Vault

// COMMAND ----------

import org.apache.spark.eventhubs._

val eventHubConnectionString = ConnectionStringBuilder()
                                    .setNamespaceName("difinitytel")
                                    .setEventHubName("towerfeed")
                                    .setSasKeyName("RootManageSharedAccessKey")
                                    .setSasKey(dbutils.secrets.get(scope = "DifinityTelecomKV", key = "DifinityTel-EventHub-TowerData"))
                                    .build                                   

// COMMAND ----------

// MAGIC %md
// MAGIC ### Configure the streaming Event Hub receiver

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.eventhubs._
import org.apache.spark.sql.streaming.{OutputMode, ProcessingTime, Trigger}

spark.sparkContext.setLocalProperty("spark.scheduler.pool", "pool1")
spark.conf.set("spark.sql.shuffle.partitions", "2")

val incomingEhConf = EventHubsConf(eventHubConnectionString)
  .setStartingPosition(EventPosition.fromEndOfStream)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Load the reference data for enrichment
// MAGIC 
// MAGIC In this example, the reference data is in blob files.  Normally these would exist in a database or datastore and would not be static

// COMMAND ----------

val towersDF = sqlContext.read.option("multiline", "true").json("/mnt/reference/towers.json")
val websitesDF = sqlContext.read.option("multiline", "true").json("/mnt/reference/websites.json")
val subscribersDF = sqlContext.read.option("multiline", "true").json("/mnt/reference/subscribers.json")
val dataspecialsDF = sqlContext.read.option("multiline", "true").json("/mnt/reference/dataspecials.json")

// COMMAND ----------

val eventHubFeedDF = spark
  .readStream
  .format("org.apache.spark.sql.eventhubs.EventHubsSourceProvider")
  .options(incomingEhConf.toMap)
  .load()
  .select($"body" cast "string" as "json")
  .select(from_json($"json", Encoders.product[CallDetailRecord].schema) as "record")
  .selectExpr("record.*")
  .withWatermark("eventDate","600 seconds")  

// COMMAND ----------

// MAGIC %md
// MAGIC ## Enrich the dataframe using scala

// COMMAND ----------

val enrichedCDRDF = eventHubFeedDF
  .join(towersDF,Seq("towerId"),"left_outer")
  .join(subscribersDF,eventHubFeedDF.col("fromNumber") === subscribersDF.col("SubscriberNumber"),"left_outer")
  .join(websitesDF,Seq("uri"),"left_outer")
  .join(dataspecialsDF,Seq("uri"),"left_outer")

// COMMAND ----------

// MAGIC %md
// MAGIC ### Enrich the dataframe using SQL

// COMMAND ----------

// MAGIC %md
// MAGIC #### Persist the dataframes as sql temporary tables

// COMMAND ----------

eventHubFeedDF.createOrReplaceTempView("eventHubFeed")
towersDF.createOrReplaceTempView("towers")
websitesDF.createOrReplaceTempView("websites")
subscribersDF.createOrReplaceTempView("subscribers")
dataspecialsDF.createOrReplaceTempView("dataspecials")

// COMMAND ----------

// MAGIC %md
// MAGIC #### Use SQL query with window aggregation to create summary view of data

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE OR REPLACE TEMPORARY VIEW SubscriberSummaryCalls
// MAGIC AS
// MAGIC SELECT
// MAGIC     MIN(date_format(eventHubFeed.eventDate, "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")) as windowstart,
// MAGIC     MAX(date_format(eventHubFeed.eventDate, "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")) as windowend,
// MAGIC     "call" as billingtype,
// MAGIC     eventHubFeed.fromNumber as subscriberid,
// MAGIC     eventHubFeed.toNumber as callednumber,
// MAGIC     sum(eventHubFeed.duration) as callduration,
// MAGIC     null as smscount,
// MAGIC     null as uri,
// MAGIC     null as mb,
// MAGIC     null as costpermb,
// MAGIC     null as uricost,
// MAGIC     null as dataoffer
// MAGIC FROM
// MAGIC     eventHubFeed
// MAGIC       LEFT JOIN subscribers ON eventHubFeed.fromNumber = subscribers.SubscriberNumber
// MAGIC WHERE
// MAGIC     billingType = 'call'
// MAGIC GROUP BY
// MAGIC     eventHubFeed.towerId,
// MAGIC     eventHubFeed.fromNumber,
// MAGIC     eventHubFeed.toNumber,
// MAGIC     window(eventHubFeed.eventDate, "600 seconds") 

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM SubscriberSummaryCalls

// COMMAND ----------

// MAGIC %md
// MAGIC #### wrap sql view into a dataframe

// COMMAND ----------

val subscriberSummaryDF = spark.sql("SELECT * FROM SubscriberSummary")

// COMMAND ----------

// MAGIC %md
// MAGIC ### Configure Cosmos DB sink

// COMMAND ----------

// MAGIC %md Write batches out to Cosmos DB

// COMMAND ----------

import com.microsoft.azure.cosmosdb.spark._
import com.microsoft.azure.cosmosdb.spark.schema._
import com.microsoft.azure.cosmosdb.spark.streaming._
import com.microsoft.azure.cosmosdb.spark.config._
import com.microsoft.azure.cosmosdb.Document

val configMap = Map(
  "Endpoint" -> "https://difinitytel.documents.azure.com:443/",
  "Masterkey" -> dbutils.secrets.get(scope = "DifinityTelecomKV", key = "DifinityTel-CosmosDB-UserProfiles"),
  "Database" -> "difinitytelecom",
  "collection" -> "usersummary",
  "checkpointLocation" -> "/towerfeed/warmpath")

subscriberSummaryDF
  .writeStream
  .format(classOf[CosmosDBSinkProvider].getName)
  .outputMode("append")
  .options(configMap)
  .start()

// COMMAND ----------

