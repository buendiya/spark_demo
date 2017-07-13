package jingsz

import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession


object SimpleSqlApp {
  def main(args: Array[String]) {
    // warehouseLocation points to the default location for managed databases and tables
    val warehouseLocation = "spark-warehouse"

    val spark = SparkSession
      .builder()
      .appName("Spark Hive Example")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    import spark.sql

    // Queries are expressed in HiveQL
    sql("SELECT * FROM dwd_logs.dwd_mp_sdk where dt='20170615' and hour='15' limit 2").show()

  }
}

