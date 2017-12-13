import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object ExtractJSONFromHiveTable {

  var appName = "ExtractJSONFromHiveTable"
 // var master = "local"
  // The schema is encoded in a string
  val schemaString = "name type subscription_id"

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName(appName)//.setMaster(master)
    val sc = new SparkContext(conf)
    val hc = new org.apache.spark.sql.hive.HiveContext(sc)

    // Read data from Hive table
    val notificationsData = hc.sql("select document from myqdatawarehouse.myquserdata_entry where instr(document_id, 'subscription_') != 0 limit 10")

    //Convert it to JSON
    val jsonData = hc.read.json(notificationsData.map(r=> r.getString(0)))

    //Select attributes
    val record = jsonData.select(jsonData("name"),jsonData("type"),jsonData("subscription_id"))

    // Convert records of the RDD to Rows.
    val rowRDD = record.map(r => Row(r.getString(0),r.getString(1),r.getString(2))) // Row(r(0), r(1)))

    // Generate the schema
    val schema = StructType(Array(StructField("name",StringType,true),
                                 StructField("type",StringType,true),
                                 StructField("subscription_id",StringType,true))
                          )

    // Apply the schema to the RDD.
    val notificationDataFrame = hc.createDataFrame(rowRDD, schema)

    // Register the DataFrames as a table.

    notificationDataFrame.write.mode(SaveMode.Overwrite).saveAsTable("myqdatawarehouse.notifications")

  //  println("Total records " + count)
  }

}
