import org.apache.spark.{SparkConf, SparkContext}

object ETLIntoHive {

  var appName = "ETLIntoHive"
  var master = "local"

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(appName).setMaster(master)
    val sc = new SparkContext(conf)
    val hc = new org.apache.spark.sql.hive.HiveContext(sc)

    val trip_data_summary = hc.sql("select `start station`, `end station`, count(*) as trips from `default`.`trip_data` group by `start station`, `end station` order by trips desc")

    trip_data_summary.registerTempTable("trip_data_summary")

    hc.sql("create table zee_trip_summary as select * from trip_data_summary")
  }

}
