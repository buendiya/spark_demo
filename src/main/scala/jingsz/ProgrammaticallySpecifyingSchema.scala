package jingsz

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.types._


class MplogInfo(logFile: String) {
  val source = scala.io.Source.fromFile(logFile)
  val content = try source.mkString finally source.close()
  val lines = content.split("\n")
  val schema = lines(0)
  val mpLogs = lines.slice(1, lines.length)
}


object ProgrammaticallySpecifyingSchema {
  def main(args: Array[String]) {
    val mpLogFile = "/Users/jingsz/IdeaProjects/sparkstreaming/demo/src/main/resources/mp_log.txt" // Should be some file on your system

    // warehouseLocation points to the default location for managed databases and tables
    val warehouseLocation = "spark-warehouse"
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Spark Example")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .getOrCreate()

    import spark.implicits._
    import spark.sql

    val mplogInfo = new MplogInfo(mpLogFile)

    val fields = mplogInfo.schema.split("\t")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)

    val rowRDD = spark.sparkContext
      .makeRDD(mplogInfo.mpLogs)
      .map(_.split("\t"))
      .map(attributes => Row(attributes(0), attributes(1), attributes(2), attributes(3)))

    val mpLogDF = spark.createDataFrame(rowRDD, schema)
    mpLogDF.createOrReplaceTempView("MpSdk")
    mpLogDF.show()
  }

}
