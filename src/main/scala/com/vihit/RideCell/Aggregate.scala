package com.vihit.RideCell

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Aggregate {
  
  def main(args:Array[String]):Unit = {
    
    val spark = SparkSession.builder().appName("Listen").enableHiveSupport().getOrCreate()

    val kafkaDf = spark.readStream.format("kafka").option("subscribe","rideCellInput")
    .option("kafka.bootstrap.servers","localhost:9092").load
    
    import spark.implicits._
    
    
    val webData = kafkaDf.select(col("value").cast("String").as("value"))
    .select(Utilities.splitColumns(4, "value", ",", Array("time","_type","incoming_url","microservice")):_*)
    
    //(Utilities.splitColumns(4, "value", ",", Array("time","_type","incoming_url","microservice")):_*)
    //split(col("value"),",")(0).as("time"),split(col("value"),",")(1).as("_type"),split(col("value"),",")(2).as("incoming_url"),split(col("value"),",")(3).as("microservice")
    
    val min1agg = webData
    .select(from_utc_timestamp(to_utc_timestamp(from_unixtime(unix_timestamp('time,"yyyy-MM-dd'T'HH:mm:ss.SSSZZZZZ")),"IST"),"MST").as("time"),'incoming_url,'_type,'microservice)
    .groupBy(window('time,"1 minutes")).count
    .selectExpr(Utilities.concatColumns(",", Array("window.start","window.end","count"), "value"))
        
        //concat($"window.start",lit(","),$"window.end",lit(","),'count).as("value"))
    
    
    val query1 = min1agg.writeStream.format("kafka").option("kafka.bootstrap.servers","localhost:9092")
    .option("checkpointLocation","/home/vihit/Desktop/checkpoint/").outputMode("update").option("topic","rideCellOutput")
    
    val min5agg = webData
    .select(from_utc_timestamp(to_utc_timestamp(from_unixtime(unix_timestamp('time,"yyyy-MM-dd'T'HH:mm:ss.SSSZZZZZ")),"IST"),"MST").as("time"),'incoming_url,'_type,'microservice)
    .groupBy(window('time,"5 minutes")).count
    .selectExpr(Utilities.concatColumns(",", Array("window.start","window.end","count"), "value"))
    
    //.select(concat($"window.start",lit(","),$"window.end",lit(","),'count).as("value"))
    //println((Utilities.concatColumns(",", Array("window.start","window.end","count"), "value")))
    
    
    val query2 = min5agg.writeStream.format("kafka").option("kafka.bootstrap.servers","localhost:9092")
    .option("checkpointLocation","/home/vihit/Desktop/checkpoint1/").outputMode("update").option("topic","rideCellOutput")
    
    val min1typeagg = webData
    .select(from_utc_timestamp(to_utc_timestamp(from_unixtime(unix_timestamp('time,"yyyy-MM-dd'T'HH:mm:ss.SSSZZZZZ")),"IST"),"MST").as("time"),'incoming_url,'_type,'microservice)
    .groupBy(window('time,"1 minutes"),'_type).count
    .selectExpr(Utilities.concatColumns(",", Array("window.start","window.end","_type","count"), "value"))
        
        //concat($"window.start",lit(","),$"window.end",lit(","),'count).as("value"))
    
    val query3 = min1typeagg.writeStream.format("kafka").option("kafka.bootstrap.servers","localhost:9092")
    .option("checkpointLocation","/home/vihit/Desktop/checkpoint2/").outputMode("update").option("topic","rideCellOutput")
    
    val min5typeagg = webData
    .select(from_utc_timestamp(to_utc_timestamp(from_unixtime(unix_timestamp('time,"yyyy-MM-dd'T'HH:mm:ss.SSSZZZZZ")),"IST"),"MST").as("time"),'incoming_url,'_type,'microservice)
    .groupBy(window('time,"5 minutes"),'_type).count
    .selectExpr(Utilities.concatColumns(",", Array("window.start","window.end","_type","count"), "value"))
      
    val query4 = min5typeagg.writeStream.format("kafka").option("kafka.bootstrap.servers","localhost:9092")
    .option("checkpointLocation","/home/vihit/Desktop/checkpoint3/").outputMode("update").option("topic","rideCellOutput")
    
  
    val min1typeurlagg = webData
    .select(from_utc_timestamp(to_utc_timestamp(from_unixtime(unix_timestamp('time,"yyyy-MM-dd'T'HH:mm:ss.SSSZZZZZ")),"IST"),"MST").as("time"),'incoming_url,'_type,'microservice)
    .groupBy(window('time,"1 minutes"),'_type,'incoming_url).count
    .select(concat($"window.start",lit(","),$"window.end",lit(","),$"_type",lit(","),$"incoming_url",'count).as("value"))
    
    //.selectExpr(Utilities.concatColumns(",", Array("window.start","window.end","_type","incoming_url","count"), "value"))
      
    val query5 = min1typeurlagg.writeStream.format("kafka").option("kafka.bootstrap.servers","localhost:9092")
    .option("checkpointLocation","/home/vihit/Desktop/checkpoint4/").outputMode("update").option("topic","rideCellOutput")
  
    query5.start()
    
    query4.start()
    
    query3.start()
    
    query2.start()
    
    query1.start().awaitTermination()
    
    
  }
}