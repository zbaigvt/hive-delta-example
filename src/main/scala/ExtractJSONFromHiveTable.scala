import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.types._

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
                                       jsonData("time_blocks.values.from_time").getItem(0).alias("from_time"),
                                       jsonData("time_blocks.values.to_time").getItem(0).alias("to_time"),
                                        explode(jsonData("time_blocks.values.day")).alias("days"))


    val notificationTypesRecord =  jsonData.select(
                                       jsonData("document_id").alias("notify_doc_id"),
                                       explode(jsonData("notification_types.values")).alias("notification_types"))

    //TODO join RDDs to produce one result + update schema with new changes
    val joinEvents = masterRecord.join(eventsRecord,masterRecord("master_doc_id")===eventsRecord("event_doc_id"), "left_outer")
    val joinEmail = joinEvents.join(emailNotificationRecord,joinEvents("master_doc_id")===emailNotificationRecord("email_doc_id"),"left_outer")
    val joinNotifyTypes = joinEmail.join(notificationTypesRecord,joinEmail("master_doc_id")===notificationTypesRecord("notify_doc_id"),"left_outer")
    val joinTimeBlocks = joinNotifyTypes.join(timeBlocksRecord,joinNotifyTypes("notify_doc_id")===timeBlocksRecord("time_doc_id"),"left_outer")


    //	0	 |-- master_doc_id: string (nullable = true)
    //	1	 |-- type: string (nullable = true)
    //	2	 |-- subscription_id: string (nullable = true)
    //	3	 |-- access_schedule_id: string (nullable = true)
    //	4	 |-- account_users: string (nullable = true)
    //	5	 |-- access_groups: string (nullable = true)
    //	6	 |-- nest_zone_ids: string (nullable = true)
    //	7	 |-- name: string (nullable = true)
    //	8	 |-- enabled: boolean (nullable = true)
    //	9	 |-- notification_delay_minutes: long (nullable = true)
    //	10	 |-- created_on_time: string (nullable = true)
    //	11	 |-- updated_on_time: string (nullable = true)
    //	12	 |-- device_serial_numbers: string (nullable = true)
    //	13	 |-- event_doc_id: string (nullable = true)
    //	14	 |-- events: string (nullable = true)
    //	15	 |-- email_doc_id: string (nullable = true)
    //	16	 |-- email_notification_list: string (nullable = true)
    //	17	 |-- notify_doc_id: string (nullable = true)
    //	18	 |-- notification_types: string (nullable = true)
    //	19	 |-- time_doc_id: string (nullable = true)
    //	20	 |-- from_time: string (nullable = true)
    //	21	 |-- to_time: string (nullable = true)
    //	22	 |-- days: string (nullable = true)

    // Convert records of the RDD to Rows.
    val rowRDD = joinTimeBlocks.map(r => Row(r.getString(0),
                                            r.getString(1),
                                            r.getString(2),
                                            r.getString(3),
                                            r.getString(4),
                                            r.getString(5),
                                            r.getString(6),
                                            r.getString(7),
                                            r.getBoolean(8),
                                            r.getLong(9),
                                            r.getString(10),
                                            r.getString(11),
                                            r.getString(12),
                                            r.getString(14), //event //13 is event_doc_id
                                            r.getString(14),//event
                                            r.getString(14), //event
                                            r.getString(14), //event
                                            r.getString(16), //email_notifications_list 15 is email_doc_id
                                            r.getString(18), //notification_types
                                            r.getString(18), //notification_types
                                            r.getString(20),
                                            r.getString(21),
                                            r.getString(22)) //days
                                          )

    // Generate the schema
    val schema = StructType(Array(StructField("document_id",StringType,true),
                                  StructField("type",StringType,true),
                                  StructField("subscription_id",StringType,true),
                                  StructField("access_schedule_id",StringType,true),
                                  StructField("account_users",StringType,true),
                                  StructField("access_groups",StringType,true),
                                  StructField("nest_zone_ids",StringType,true),
                                  StructField("name",StringType,true),
                                  StructField("enabled",BooleanType,true),
                                  StructField("notification_delay_minutes",LongType,true),
                                  StructField("created_on_time",StringType,true),
                                  StructField("updated_on_time",StringType,true),
                                  StructField("device_serial_number",StringType,true),
                                  StructField("event_open",StringType,true),
                                  StructField("event_close",StringType,true),
                                  StructField("events_on",StringType,true),
                                  StructField("event_off",StringType,true),
                                  StructField("email_notification_list",StringType,true),
                                  StructField("push_notification",StringType,true),
                                  StructField("email_notification",StringType,true),
                                  StructField("from_time",StringType,true),
                                  StructField("to_time",StringType,true),
                                  StructField("days_of_week",StringType,true)
                                )
                          )

    // Apply the schema to the RDD.
    val notificationDataFrame = hc.createDataFrame(rowRDD, schema)

    // Register the DataFrames as a table.

    notificationDataFrame.write.mode(SaveMode.Overwrite).saveAsTable("myqdatawarehouse.notifications")

  //  println("Total records " + count)
  }

}
