// Databricks notebook source
// MAGIC %md
// MAGIC # Difinity Telecom - Basic Stream Processing
// MAGIC 
// MAGIC ## Event Hubs >> Azure Databricks 
// MAGIC 
// MAGIC A realtime stream from Azure Event Hubs is enriched and displayed in a Databricks Notebook

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Azure Event Hubs using the spark streaming library
// MAGIC 
// MAGIC The source code for the Spark Event Hubs connection can be found here:
// MAGIC [https://github.com/Azure/azure-event-hubs-spark](https://github.com/Azure/azure-event-hubs-spark)
// MAGIC 
// MAGIC The maven co-ordinates for the current version of the Spark Event Hubs connector:
// MAGIC [com.microsoft.azure:azure-eventhubs-spark_2.11:2.3.9](https://search.maven.org/artifact/com.microsoft.azure/azure-eventhubs-spark_2.11/2.3.9/jar)

// COMMAND ----------

// imports at the top
import org.apache.spark.eventhubs._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.eventhubs._
import org.apache.spark.sql.streaming.{OutputMode, ProcessingTime, Trigger}
import java.sql.Timestamp
import java.time._
import java.time.temporal

// COMMAND ----------

// MAGIC %md
// MAGIC ### Create a java class for the Call Detail Record

// COMMAND ----------

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

val eventHubConnectionString = ConnectionStringBuilder()
                                    .setNamespaceName("difinitytel")
                                    .setEventHubName("towerfeed")
                                    .setSasKeyName("RootManageSharedAccessKey")
                                    .setSasKey(dbutils.secrets.get(scope = "DifinityTelecomKV", key = "DifinityTel-EventHub-TowerData"))
                                    .build                                   

// COMMAND ----------

// MAGIC %md
// MAGIC ### Configure when the streaming Event Hub receiver starts receiving events from

// COMMAND ----------

//val starttime = Instant.parse("2019-04-29T10:00:00Z")
val starttime = Instant.now.minusSeconds(600)

val incomingEhConf = EventHubsConf(eventHubConnectionString)
  .setStartingPosition(EventPosition.fromEndOfStream)
  //.setStartingPosition(EventPosition.fromEnqueuedTime(starttime))
  .setConsumerGroup("databricks-pbi")

// COMMAND ----------

// MAGIC %md
// MAGIC ### Load reference data for stream enrichment
// MAGIC 
// MAGIC In this case we are joining the raw telemetry stream with a list of Cell Towers, so we can use the address and/or latitude+longitude in our Power BI dashboard

// COMMAND ----------

val towersDF = sqlContext.read.option("multiline", "true").json("/mnt/reference/towers.json")

// COMMAND ----------

display(towersDF)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Read stream from Event Hubs into a Spark Dataframe, join to reference data

// COMMAND ----------

val eventInboundDF = spark
  .readStream
  .format("org.apache.spark.sql.eventhubs.EventHubsSourceProvider")
  .options(incomingEhConf.toMap)
  .load()
  .select($"body" cast "string" as "json")
  .select(from_json($"json", Encoders.product[CallDetailRecord].schema) as "record")
  .selectExpr("record.*")
  .join(towersDF,Seq("towerId"),"left_outer")

// COMMAND ----------

display(eventInboundDF)

// COMMAND ----------

eventInboundDF.createOrReplaceTempView("streamingEvents")

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT
// MAGIC   SUM(bytes),
// MAGIC   COUNT(1),
// MAGIC   Name
// MAGIC FROM
// MAGIC   streamingEvents
// MAGIC GROUP BY
// MAGIC   Name
// MAGIC   