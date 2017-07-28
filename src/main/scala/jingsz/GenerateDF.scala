package jingsz

import org.apache.spark.sql.{Dataset, Row, SparkSession}


case class Person(name: String, age: Long)


object GenerateDF {
  def main(args: Array[String]) {
    val peopleFile = "/Users/jingsz/IdeaProjects/sparkstreaming/demo/src/main/resources/people.txt" // Should be some file on your system
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

    val peopleDF = spark.sparkContext
      .textFile(peopleFile)
      .map(_.split(","))
      .map(attributes => Person(attributes(0), attributes(1).trim.toInt))
      .toDF()
    // Register the DataFrame as a temporary view
    peopleDF.createOrReplaceTempView("people")

    // SQL statements can be run by using the sql methods provided by Spark
    val teenagersDF = sql("SELECT name, age FROM people WHERE age BETWEEN 13 AND 39")

    // The columns of a row in the result can be accessed by field index
    teenagersDF.map(teenager => "Name: " + teenager(0)).show()

    teenagersDF.filter($"age" > 26)

  }
}
