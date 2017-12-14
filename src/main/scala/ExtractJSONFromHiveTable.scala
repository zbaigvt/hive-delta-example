import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object ExtractJSONFromHiveTable {

  var appName = "ExtractJSONFromHiveTable"
 // var master = "local"
  // The schema is encoded in a string
  val schemaString = "name type subscription_id"

  def convertScheduleToString(scheduleList: java.util.List[Nothing]): String = {
    val builder = StringBuilder.newBuilder;
    if(scheduleList != null && !scheduleList.isEmpty) {
      if(scheduleList.contains("monday")) {
        builder.append("M")
      }
      else  {
        builder.append("m")
      }
      if(scheduleList.contains("tuesday")) {
        builder.append("T")
      }
      else  {
        builder.append("t")
      }
      if(scheduleList.contains("wednesday")) {
        builder.append("W")
      }
      else  {
        builder.append("w")
      }
      if(scheduleList.contains("thursday")) {
        builder.append("R")
      }
      else  {
        builder.append("r")
      }
      if(scheduleList.contains("friday")) {
        builder.append("F")
      }
      else  {
        builder.append("f")
      }
      if(scheduleList.contains("saturday")) {
        builder.append("S")
      }
      else  {
        builder.append("s")
      }
      if(scheduleList.contains("sunday")) {
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

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName(appName)//.setMaster(master)
    val sc = new SparkContext(conf)
    val hc = new org.apache.spark.sql.hive.HiveContext(sc)

    // Read data from Hive table.adding document_id to JSON structure to easily retrieve later in jsonData RDD
    val notificationsData = hc.sql("select concat('{\"document_id\":','\"',document_id,'\", ',substring(document,2)) as document from myqdatawarehouse.myquserdata_entry where instr(document_id, 'subscription_') != 0 limit 10")

    //Convert it to JSON
    val jsonData = hc.read.json(notificationsData.map{r=> r.getString(0)})

    val masterRecord =  jsonData.select(
                                   jsonData("document_id").alias("master_doc_id"),
                                   jsonData("type"),
                                   jsonData("subscription_id"),
                                   jsonData("access_schedule_id"),
                                   jsonData("account_users"),
                                   jsonData("access_groups"),
                                   jsonData("nest_zone_ids"),
                                   jsonData("name"),
                                   jsonData("enabled"),
                                   jsonData("notification_delay_minutes"),
                                   jsonData("created_on_time"),
                                   jsonData("updated_on_time"),
                                   explode(jsonData("device_serial_numbers.values")).alias("device_serial_numbers")
                                   )

    val eventsRecord = jsonData.select(
                                    jsonData("document_id").alias("event_doc_id"),
                                    explode(jsonData("events.values")).alias("events"))

    val emailNotificationRecord =  jsonData.select(
                                         jsonData("document_id").alias("email_doc_id"),
                                         explode(jsonData("email_notification_list.values")).alias("email_notification_list"))

    val timeBlocksRecord = jsonData.select(
                                       jsonData("document_id").alias("time_doc_id"),
                                       jsonData("time_blocks.values.from_time").getItem(0),
                                       jsonData("time_blocks.values.to_time").getItem(0),
                                        explode(jsonData("time_blocks.values.day")).alias("days"))


    val notificationTypesRecord =  jsonData.select(
                                       jsonData("document_id").alias("notify_doc_id"),
                                       explode(jsonData("notification_types.values")).alias("notification_types"))

    //TODO join RDDs to produce one result + update schema with new changes
    val joinEvents = masterRecord.join(eventsRecord,masterRecord("master_doc_id")===eventsRecord("event_doc_id"), "left_outer")
    val joinEmail = joinEvents.join(emailNotificationRecord,joinEvents("master_doc_id")===emailNotificationRecord("email_doc_id"),"left_outer")
    val joinNotifyTypes = joinEmail.join(notificationTypesRecord,joinEmail("master_doc_id")===notificationTypesRecord("notify_doc_id"),"left_outer")



    // Convert records of the RDD to Rows.
    val rowRDD = joinNotifyTypes.map(r => Row(r.getString(0),
                                            r.getString(1),
                                            r.getString(2),
                                            r.getString(3),
                                            r.getString(4),
                                            r.getString(6)))

    // Generate the schema
    val schema = StructType(Array(StructField("document_id",StringType,true),
                                  StructField("name",StringType,true),
                                  StructField("type",StringType,true),
                                  StructField("subscription_id",StringType,true),
                                  StructField("access_schedule_id",StringType,true),
                                  StructField("account_users",StringType,true),
                                  StructField("access_groups",StringType,true),
                                  StructField("email_notification_list",StringType,true),
                                  StructField("nest_zone_ids",StringType,true),
                                  StructField("enabled",StringType,true),
                                  StructField("notification_delay_minutes",StringType,true),
                                  StructField("event_open",StringType,true),
                                  StructField("event_close",StringType,true),
                                  StructField("events_on",StringType,true),
                                  StructField("event_off",StringType,true),
                                  StructField("access_events",StringType,true),
                                  StructField("days_of_week",StringType,true),
                                  StructField("from_time",StringType,true),
                                  StructField("to_time",StringType,true),
                                  StructField("account_user_id",StringType,true),
                                  StructField("device_serial_number",StringType,true),
                                  StructField("push_notification",StringType,true),
                                  StructField("email_notification",StringType,true),
                                  StructField("created_on_time",StringType,true),
                                  StructField("updated_on_time",StringType,true),
                                  StructField("is_default",StringType,true),
                                  StructField("entry_code_uses_exceeded",StringType,true),
                                  StructField("entry_code_uses_exceeded_id",StringType,true),
                                  StructField("entry_code_uses_exceeded_amount",StringType,true),
                                  StructField("entry_code_uses_exceeded_time_period",StringType,true)
                                )
                          )

    // Apply the schema to the RDD.
    val notificationDataFrame = hc.createDataFrame(rowRDD, schema)

    // Register the DataFrames as a table.

    notificationDataFrame.write.mode(SaveMode.Overwrite).saveAsTable("myqdatawarehouse.notifications")

  //  println("Total records " + count)
  }

}
