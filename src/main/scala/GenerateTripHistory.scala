import org.apache.spark.sql.SaveMode
import org.apache.spark.{SparkConf, SparkContext}


object GenerateTripHistory {

  var appName = "GenerateTripHistory"
  var master = "local"


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(appName).setMaster(master)
    val sc = new SparkContext(conf)
    val hc = new org.apache.spark.sql.hive.HiveContext(sc)

    hc.sql("DROP TABLE IF EXISTS default.zee_trip_history")

    val trip_data_main = hc.sql("select `trip id` as trip_id, `start date` as start_date, `duration` as duration, `start station` as start_station, hash(`trip id`, `start date`, `duration`, `start station`) as hash_key from trip_data limit 25")

    trip_data_main.registerTempTable("trip_data_main")

    val trip_data_latest = hc.sql("select `trip id` as trip_id_new, `start date` as start_date_new, `duration` as duration_new, `start station` as start_station_new, hash(`trip id`, `start date`, `duration`, `start station`) as hash_key_new from trip_data_new limit 25")

    trip_data_latest.registerTempTable("trip_data_latest")

    val trip_data_changed = hc.sql("select trip_id,start_date_new,duration_new,start_station_new, cast(current_date() as string) as record_date, '1' as flag, hash_key, hash_key_new FROM trip_data_main left join trip_data_latest on trip_data_main.trip_id = trip_data_latest.trip_id_new where hash_key <> hash_key_new")

    // trip_data_changed.write.mode(SaveMode.Append).saveAsTable("default.zee_trip_history")
    trip_data_changed.registerTempTable("zee_trip_history")

    hc.sql("create table default.zee_trip_history as select * from zee_trip_history")

  }
}
