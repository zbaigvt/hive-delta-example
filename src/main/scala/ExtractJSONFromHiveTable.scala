import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.concat_ws
import org.apache.spark.sql.types._

object ExtractJSONFromHiveTable {

  var appName = "ExtractJSONFromHiveTable"
  var master = "local"


  def convertScheduleToString(days: String): String = {
    val builder = StringBuilder.newBuilder
    if(days != null) {
      if(days.contains("Monday")) {
        builder.append("M")
      }
      else  {
        builder.append("m")
      }
      if(days.contains("Tuesday")) {
        builder.append("T")
      }
      else  {
        builder.append("t")
      }
      if(days.contains("Wednesday")) {
        builder.append("W")
      }
      else  {
        builder.append("w")
      }
      if(days.contains("Thursday")) {
        builder.append("R")
      }
      else  {
        builder.append("r")
      }
      if(days.contains("Friday")) {
        builder.append("F")
      }
      else  {
        builder.append("f")
      }
      if(days.contains("Saturday")) {
        builder.append("S")
      }
      else  {
        builder.append("s")
      }
      if(days.contains("Sunday")) {
        builder.append("U")
      }
      else  {
        builder.append("u")
      }
    } else {
      builder.append("mtwrfsu")
    }
    builder.toString
  }

  def convertStringToBoolean(dataParam: String, refString: String) : Boolean = {
    if(dataParam.equals(refString))
      return true
    else
      return false
  }
  //TODO remove explodes and replce with concat_ws
  // Remove explodes from devices as well

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName(appName).setMaster(master)
    val sc = new SparkContext(conf)
    val hc = new org.apache.spark.sql.hive.HiveContext(sc)

    // Read data from Hive table.adding document_id to JSON structure to easily retrieve later in jsonData RDD
    val notificationsData = hc.sql("select concat('{\"document_id\":','\"',document_id,'\", ',substring(document,2)) as document from myqdatawarehouse.myquserdata_entry where instr(document_id, 'subscription_') != 0 limit 10")

    //Convert it to JSON
    val jsonData = hc.read.json(notificationsData.map{r=> r.getString(0)})

    val notificationRecord   =  jsonData.select(
                                   jsonData("document_id"),
                                   jsonData("type"),
                                   jsonData("subscription_id"),
                                   jsonData("access_schedule_id"),
                                   jsonData("account_users"),
                                   jsonData("access_groups"),
                                   concat_ws(",",jsonData("email_notification_list.values")).alias("email_notification_list"),
                                   jsonData("nest_zone_ids"),
                                   jsonData("name"),
                                   jsonData("enabled"),
                                   jsonData("notification_delay_minutes"),
                                   concat_ws(",",jsonData("events.values")).alias("events"),
                                   jsonData("time_blocks.values.from_time").getItem(0).alias("from_time"),
                                   jsonData("time_blocks.values.to_time").getItem(0).alias("to_time"),
                                   concat_ws(",",jsonData("time_blocks.values.day")).alias("days"),
                                   jsonData("account_user_id"),
                                   concat_ws(",",jsonData("device_serial_numbers.values")).alias("device_serial_numbers"),
                                   concat_ws(",",jsonData("notification_types.values")).alias("notification_types"),
                                   jsonData("created_on_time"),
                                   jsonData("updated_on_time")
                                   )

 /*   ﻿root
  0  |-- document_id: string (nullable = true)
  1  |-- type: string (nullable = true)
  2  |-- subscription_id: string (nullable = true)
  3  |-- access_schedule_id: string (nullable = true)
  4  |-- account_users: string (nullable = true)
  5  |-- access_groups: string (nullable = true)
  6  |-- email_notification_list: string (nullable = false)
  7  |-- nest_zone_ids: string (nullable = true)
  8  |-- name: string (nullable = true)
  9  |-- enabled: boolean (nullable = true)
  10  |-- notification_delay_minutes: long (nullable = true)
  11  |-- events: string (nullable = false)
  12  |-- from_time: string (nullable = true)
  13 |-- to_time: string (nullable = true)
  14  |-- days: string (nullable = false)
  15  |-- account_user_id: string (nullable = true)
  16  |-- device_serial_numbers: string (nullable = false)
  17  |-- notification_types: string (nullable = false)
  18  |-- created_on_time: string (nullable = true)
  19  |-- updated_on_time: string (nullable = true)


    **/
    // Convert records of the RDD to Rows.
    val rowRDD = notificationRecord.map(r => Row(
                                            r.getString(0), //document_id
                                            r.getString(1), //type
                                            r.getString(2), //subscription_id
                                            r.getString(3), //access_schedule_id
                                            r.getString(4), // account_users
                                            r.getString(5), //access_groups
                                            r.getString(6), //email_notification_list
                                            r.getString(7), //nest_zone_ids
                                            r.getString(8), //name
                                            r.getBoolean(9), //enabled
                                            r.getString(16), //device_serial_numbers
                                            r.getString(15), //account_user_id
                                            r.getLong(10), //notification_delay_minutes
                                            convertStringToBoolean(r.getString(11).toLowerCase, "open"), //event open
                                            convertStringToBoolean(r.getString(11).toLowerCase, "closed"), //event closed
                                            convertStringToBoolean(r.getString(11).toLowerCase, "on"), //event on
                                            convertStringToBoolean(r.getString(11).toLowerCase, "off"), //event off
                                            convertStringToBoolean(r.getString(17).toLowerCase,"pushnotification"), //notification_types﻿PushNotification
                                            convertStringToBoolean(r.getString(17).toLowerCase,"email"), //notification_types ﻿Email
                                            r.getString(12), //from_time
                                            r.getString(13), //to_time
                                            convertScheduleToString(r.getString(14)), //days_of_week
                                            r.getString(18), // created_on_time
                                            r.getString(19) //updated_on_time
                                          ))

    // Generate the schema
    val schema = StructType(Array(StructField("document_id",StringType,true),
                                  StructField("type",StringType,true),
                                  StructField("subscription_id",StringType,true),
                                  StructField("access_schedule_id",StringType,true),
                                  StructField("account_users",StringType,true),
                                  StructField("access_groups",StringType,true),
                                  StructField("email_notification_list",StringType,true),
                                  StructField("nest_zone_ids",StringType,true),
                                  StructField("name",StringType,true),
                                  StructField("enabled",BooleanType,true),
                                  StructField("device_serial_number",StringType,true),
                                  StructField("account_user_id",StringType,true),
                                  StructField("notification_delay_minutes",LongType,true),
                                  StructField("event_open",BooleanType,true),
                                  StructField("event_closed",BooleanType,true),
                                  StructField("events_on",BooleanType,true),
                                  StructField("event_off",BooleanType,true),
                                  StructField("push_notification",BooleanType,true),
                                  StructField("email_notification",BooleanType,true),
                                  StructField("from_time",StringType,true),
                                  StructField("to_time",StringType,true),
                                  StructField("days_of_week",StringType,true),
                                  StructField("created_on_time",StringType,true),
                                  StructField("updated_on_time",StringType,true)
                                )
                          )

    // Apply the schema to the RDD.
    val notificationDataFrame = hc.createDataFrame(rowRDD, schema)

    // Register the DataFrames as a table.

    notificationDataFrame.write.mode(SaveMode.Overwrite).saveAsTable("myqdatawarehouse.notifications")

  //  println("Total records " + count)
  }

}
