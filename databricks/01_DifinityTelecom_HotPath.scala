// Databricks notebook source
// MAGIC %md
// MAGIC # Difinity Telecom - Hot Path Processing
// MAGIC 
// MAGIC ## Event Hubs >> Azure Databricks >> Power BI
// MAGIC 
// MAGIC In the hot path, a realtime stream from Azure Event Hubs is enriched and pushed to Power BI using the Streaming Dataset API

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
// MAGIC ### Define a "foreachwriter" to output rows directly to Power BI Customer Streaming Data API

// COMMAND ----------

import org.apache.http.client.config.RequestConfig
import org.apache.http.impl.client.{HttpClientBuilder}
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.client.methods.HttpPost
import org.apache.http.client.entity.EntityBuilder
import org.apache.http.HttpResponse
import org.apache.http.entity.StringEntity
import scala.util.parsing.json.JSONObject
import org.apache.spark.sql.{ForeachWriter, Row}
import java.time.LocalDateTime
class PowerBIAPIWriter extends ForeachWriter[Row] {
  
  // configure the Power BI API Endpoint
  val PowerBIAPIEndpoint_DifinityTelecomTowerFeedLive = "https://api.powerbi.com/beta/72f988bf-86f1-41af-91ab-2d7cd011db47/datasets/80a000d2-b407-456b-afe9-808e6cd639af/rows?key=wYVvNVV%2Bj%2Fz5d6jMF9P2uV3S5JnK4wnc5ReUIf2Hxjvb3NgQfHiE3bva8vDwftad4ai%2BXLh8fwdOi0r0g6nUUA%3D%3D"

  HttpClientBuilder.create().build();
  
  //
  // This is called first when preparing to send multiple rows.
  // Put all the initialization code inside open() so that a fresh
  // copy of this class is initialized in the executor where open()
  // will be called.
  //
  def open(partitionId: Long, epochId: Long) = {
    true
  }
  
  
  //
  // This is called for each row after open() has been called.
  // This implementation sends one row at a time.
  // A more efficient implementation can be to send batches of rows at a time.
  //
  def process(row: Row) = {
    val rowAsMap = row.getValuesMap(row.schema.fieldNames)
    val dummyrow = "[{\"eventDate\" : \""+LocalDateTime.now()+"\", \"stringData\": \""+LocalDateTime.now()+"\"}]"
    
    val rowToSend = "[{\"towerId\" :\""+row.getAs("towerId")+"\","+
                    "\"eventDate\" :\""+row.getAs("eventDate")+"\","+
                    "\"towername\" :\""+row.getAs("Name")+"\","+
                    "\"toweraddress\" :\""+row.getAs("Address")+"\","+
                    "\"towercity\" :\""+row.getAs("City")+"\","+
                    "\"towerlongitude\" :\""+row.getAs("Longitude")+"\","+
                    "\"towerlatitude\" :\""+row.getAs("Latitude")+"\","+
                    "\"imei\" :\"\","+
                    "\"fromNumber\" :\""+row.getAs("fromNumber")+"\","+
                    "\"toNumber\" :\""+row.getAs("toNumber")+"\","+
                    "\"billingType\" :\""+row.getAs("billingType")+"\","+
                    "\"duration\" :"+row.getAs("duration")+","+
                    "\"bytes\" :"+row.getAs("bytes")+","+
                    "\"protocol\" :"+row.getAs("protocol")+","+
                    "\"port\" :"+row.getAs("port")+","+
                    "\"uri\" :\""+row.getAs("uri")+"\","+
                    "\"cost\" :"+row.getAs("cost")+""+
                    "}]"
    
   // "+row.getAs("fromNumber")+"
    
    println("row to send: " + rowToSend)
        
    val post = new HttpPost(PowerBIAPIEndpoint_DifinityTelecomTowerFeedLive)
    post.setHeader("Content-type", "application/json")
    post.setEntity(new StringEntity(rowToSend))
    
    val client: CloseableHttpClient = HttpClientBuilder.create().build();  
    
    val response: HttpResponse = client.execute(post)  
    println(response.getStatusLine.getStatusCode, response.getStatusLine.getReasonPhrase)
  }

  //
  // This is called after all the rows have been processed.
  //
  def close(errorOrNull: Throwable) = {
  }
 
}


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
// MAGIC ### Read stream from Event Hubs into a Spark Dataframe, join to reference data, write each row to Power BI

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
  .writeStream
  .foreach(new PowerBIAPIWriter)
  .start()