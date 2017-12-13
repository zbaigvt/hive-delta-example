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

    // Read data from Hive table
   // val notificationsData = hc.sql("select document_id, lower(document) as document from myqdatawarehouse.myquserdata_entry where instr(document_id, 'subscription_') != 0 limit 10")
    val notificationsData = hc.sql("select concat('{\"document_id\":','\"',document_id,'\", ',substring(document,2)) as document from myqdatawarehouse.myquserdata_entry where instr(document_id, 'subscription_') != 0 limit 10")
    //Convert it to JSON
    val jsonData = hc.read.json(notificationsData.map{r=> r.getString(0)})

    //Select attributes explode(jsonData("device_serial_numbers.values")).alias("device_serial_numbers"))
    //val record = jsonData.select(jsonData("name"),jsonData("type"),jsonData("subscription_id"))
   // notificationsData("document_id")

    val record =  jsonData.select( jsonData("document_id"),
                                   jsonData("name"),
                                   jsonData("type"),
                                   jsonData("subscription_id"),
                                   explode(jsonData("device_serial_numbers.values")).alias("device_serial_numbers"))
    // need doc id to join val events = jsonData.select(jsonData)


    // Convert records of the RDD to Rows.
    val rowRDD = record.map(r => Row(r.getString(0),r.getString(1),r.getString(2),r.getString(3), r.getString(4))) // Row(r(0), r(1)))

    // Generate the schema
    val schema = StructType(Array(StructField("document_id",StringType,true),
                                 StructField("name",StringType,true),
                                 StructField("type",StringType,true),
                                 StructField("subscription_id",StringType,true),
                                 StructField("device_serial_number",StringType,true))
                          )

    // Apply the schema to the RDD.
    val notificationDataFrame = hc.createDataFrame(rowRDD, schema)

    // Register the DataFrames as a table.

    notificationDataFrame.write.mode(SaveMode.Overwrite).saveAsTable("myqdatawarehouse.notifications")

  //  println("Total records " + count)
  }

}
