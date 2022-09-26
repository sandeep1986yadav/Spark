package pck

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object withColumnExp {
  def main(args: Array[String]): Unit = {
    println("================Started============")
    println

    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val df = spark.read.format("csv").option("header", "true")
      .load("file:///C:/data/dt.txt")
    println("====================Raw Data=======================")
    df.show()

    df.withColumn(
      "tdate",
      expr("split(tdate,'-')[2]"))
      .withColumnRenamed("tdate", "year")
      .show()

    df.withColumn(
      "wdate",
      expr("split(tdate,'-')[2]"))
      .show()

    df.withColumn(
      "category",
      expr("split(tdate,'-')[2]"))
      .show()

    df.withColumn(
      "category",
      expr("split(tdate,'-')[2]"))
      .withColumn("amount", expr("amount * 2"))
      .show()

    df.withColumn(
      "status",
      expr("case when spendby ='cash' then 0 else 1 end"))
      .show()

   
      
    df.withColumn(
      "tdate",
      expr("split(tdate,'-')[2]"))
      .withColumnRenamed("tdate", "year").withColumn(
        "status",
        expr("case when spendby ='cash' then 0 else 1 end"))
      .show()
      
  }
}