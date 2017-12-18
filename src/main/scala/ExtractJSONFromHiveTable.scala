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

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName(appName).setMaster(master)
    val sc = new SparkContext(conf)
    val hc = new org.apache.spark.sql.hive.HiveContext(sc)

    // Read data from Hive table.adding document_id to JSON structure to easily retrieve later in jsonData RDD
    val notificationsData = hc.sql("select concat('{\"document_id\":','\"',document_id,'\", ',substring(document,2)) as document from myqdatawarehouse.myquserdata_entry where instr(document_id, 'subscription_') != 0 ")

    //Convert it to JSON
    val jsonData = hc.read.json(notificationsData.map{r=> r.getString(0)})

    val notificationRecord   =  jsonData.select(
                                   jsonData("document_id"),
                                   jsonData("type"),
                                   jsonData("subscription_id"),
                                   jsonData("access_schedule_id"),
                                   concat_ws(",",jsonData("account_users.values")).alias("account_users"),
                                   concat_ws(",",jsonData("access_events.values")).alias("access_events"),
                                   concat_ws(",",jsonData("access_groups.values")).alias("access_groups"),
                                   concat_ws(",",jsonData("email_notification_list.values")).alias("email_notification_list"),
                                   jsonData("nest_zone_ids"),
                                   jsonData("name"),
                                   jsonData("enabled"),
                                   jsonData("notification_delay_minutes").cast(StringType), //converting long to String due to null values
                                   concat_ws(",",jsonData("events.values")).alias("events"),
                                   jsonData("time_blocks.values.from_time").getItem(0).alias("from_time"),
                                   jsonData("time_blocks.values.to_time").getItem(0).alias("to_time"),
                                   concat_ws(",",jsonData("time_blocks.values.day")).alias("days"),
                                   jsonData("account_user_id"),
                                   concat_ws(",",jsonData("device_serial_numbers.values")).alias("device_serial_numbers"),
                                   concat_ws(",",jsonData("notification_types.values")).alias("notification_types"),
                                   jsonData("is_default"),
                                   jsonData("entry_code_uses_exceeded"),
                                   jsonData("entry_code_uses_exceeded_amount").cast(StringType), //converting long to String due to null values
                                   jsonData("entry_code_uses_exceeded_id"),
                                   jsonData("entry_code_uses_exceeded_time_period").cast(StringType), //converting long to String due to null values
                                   jsonData("created_on_time"),
                                   jsonData("created_by_user_id"),
                                   jsonData("updated_on_time"),
                                   jsonData("_corrupt_record"),
                                   jsonData("allZones"),
                                   concat_ws(",",jsonData("recipientUserIds.values")).alias("recipientUserIds"),
                                   concat_ws(",",jsonData("zones.values")).alias("zones")
                                   )

   /* root
    |-- document_id: string (nullable = true)
    |-- type: string (nullable = true)
    |-- subscription_id: string (nullable = true)
    |-- access_schedule_id: string (nullable = true)
    |-- account_users: string (nullable = false)
    |-- access_events: string (nullable = false)
    |-- access_groups: string (nullable = false)
    |-- email_notification_list: string (nullable = false)
    |-- nest_zone_ids: string (nullable = true)
    |-- name: string (nullable = true)
    |-- enabled: boolean (nullable = true)
    |-- notification_delay_minutes: string (nullable = true)
    |-- events: string (nullable = false)
    |-- from_time: string (nullable = true)
    |-- to_time: string (nullable = true)
    |-- days: string (nullable = false)
    |-- account_user_id: string (nullable = true)
    |-- device_serial_numbers: string (nullable = false)
    |-- notification_types: string (nullable = false)
    |-- is_default: boolean (nullable = true)
    |-- entry_code_uses_exceeded: boolean (nullable = true)
    |-- entry_code_uses_exceeded_amount:  string (nullable = false)
    |-- entry_code_uses_exceeded_id: string (nullable = true)
    |-- entry_code_uses_exceeded_time_period:  string (nullable = false)
    |-- created_on_time: string (nullable = true)
    |-- created_by_user_id: string (nullable = true)
    |-- updated_on_time: string (nullable = true)
    |-- _corrupt_record: string (nullable = true)
    |-- allZones: boolean (nullable = true)
    |-- recipientUserIds: string (nullable = false)
    |-- zones: string (nullable = false)
    */

    // Convert records of the RDD to Rows.
    val rowRDD = notificationRecord.map(r => Row(
                    r.getAs[String]("document_id"),
                    r.getAs[String]("type"),
                    r.getAs[String]("subscription_id"),
                    r.getAs[String]("access_schedule_id"),
                    r.getAs[String]("account_users"),
                    r.getAs[String]("access_events"),
                    r.getAs[String]("access_groups"),
                    r.getAs[String]("email_notification_list"),
                    r.getAs[String]("nest_zone_ids"),
                    r.getAs[String]("name"),
                    r.getAs[Boolean]("enabled"),
                    r.getAs[String]("device_serial_numbers"),
                    r.getAs[String]("account_user_id"),
                    r.getAs[String]("notification_delay_minutes"),
                    convertStringToBoolean(r.getAs[String]("events").toLowerCase, "open"), //event open
                    convertStringToBoolean(r.getAs[String]("events").toLowerCase, "closed"), //event closed
                    convertStringToBoolean(r.getAs[String]("events").toLowerCase, "on"), //event on
                    convertStringToBoolean(r.getAs[String]("events").toLowerCase, "off"), //event off
                    convertStringToBoolean(r.getAs[String]("notification_types").toLowerCase, "pushnotification"), //notification_types﻿PushNotification
                    convertStringToBoolean(r.getAs[String]("notification_types").toLowerCase, "email"), //notification_types ﻿Email
                    r.getAs[String]("from_time"),
                    r.getAs[String]("to_time"),
                    convertScheduleToString(r.getAs[String]("days")),
                    r.getAs[Boolean]("is_default"),
                    r.getAs[Boolean]("entry_code_uses_exceeded"),
                    r.getAs[String]("entry_code_uses_exceeded_amount"),
                    r.getAs[String]("entry_code_uses_exceeded_id"),
                    r.getAs[String]("entry_code_uses_exceeded_time_period"),
                    r.getAs[String]("created_on_time"),
                    r.getAs[String]("created_by_user_id"),
                    r.getAs[String]("updated_on_time"),
                    r.getAs[String]("_corrupt_record"),
                    r.getAs[Boolean]("allZones"),
                    r.getAs[String]("recipientUserIds"),
                    r.getAs[String]("zones")
                  ))

    // Generate the schema
    val schema = StructType(Array(StructField("document_id",StringType,true),
                                  StructField("type",StringType,true),
                                  StructField("subscription_id",StringType,true),
                                  StructField("access_schedule_id",StringType,true),
                                  StructField("account_users",StringType,true),
                                  StructField("access_events",StringType,true),
                                  StructField("access_groups",StringType,true),
                                  StructField("email_notification_list",StringType,true),
                                  StructField("nest_zone_ids",StringType,true),
                                  StructField("name",StringType,true),
                                  StructField("enabled",BooleanType,true),
                                  StructField("device_serial_number",StringType,true),
                                  StructField("account_user_id",StringType,true),
                                  StructField("notification_delay_minutes",StringType,true),
                                  StructField("event_open",BooleanType,true),
                                  StructField("event_closed",BooleanType,true),
                                  StructField("events_on",BooleanType,true),
                                  StructField("event_off",BooleanType,true),
                                  StructField("push_notification",BooleanType,true),
                                  StructField("email_notification",BooleanType,true),
                                  StructField("from_time",StringType,true),
                                  StructField("to_time",StringType,true),
                                  StructField("days_of_week",StringType,true),
                                  StructField("is_default",BooleanType,true),
                                  StructField("entry_code_uses_exceeded",BooleanType,true),
                                  StructField("entry_code_uses_exceeded_amount",StringType,true),
                                  StructField("entry_code_uses_exceeded_id",StringType,true),
                                  StructField("entry_code_uses_exceeded_time_period",StringType,true),
                                  StructField("created_on_time",StringType,true),
                                  StructField("created_by_user_id",StringType,true),
                                  StructField("updated_on_time",StringType,true),
                                  StructField("corrupt_record",StringType,true),
                                  StructField("allZones",BooleanType,true),
                                  StructField("recipientUserIds",StringType,true),
                                  StructField("zones",StringType,true)
                                  )
                              )

    // Apply the schema to the RDD.
    val notificationDataFrame = hc.createDataFrame(rowRDD, schema)

    // Register the DataFrames as a table.

    notificationDataFrame.write.mode(SaveMode.Overwrite).saveAsTable("myqdatawarehouse.notifications")

    println("Process Completed ")
  }

}
