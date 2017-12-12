import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.explode

object ExtractJSONFromHiveTable {

  var appName = "ExtractJSONFromHiveTable"
  var master = "local"

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName(appName).setMaster(master)
    val sc = new SparkContext(conf)
    val hc = new org.apache.spark.sql.hive.HiveContext(sc)

    val notificationsData = hc.sql("select document_id, lower(document) as document from myqdatawarehouse.myquserdata_entry where instr(document_id, 'subscription_') != 0 limit 10")

    val jsonData = hc.read.json(notificationsData.map{r=> r.getString(1)})

    val count = jsonData.count()

    val record = jsonData.select(jsonData("type"), explode(jsonData("events.values")).alias("events"))

    record.show(5)
    println("Total records " + count)
  }

}
