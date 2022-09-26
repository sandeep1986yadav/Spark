package pck

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object grpByAgg {
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

    df.groupBy("category").count().show()

    df.groupBy("category").
      agg(count("category").as("count"))
      .orderBy("category")
      .show()

    df.groupBy("category").
      agg(sum("amount").as("Total"))
      .orderBy("category")
      .show()

    df.groupBy("category").
      agg(sum("amount").as("Total"))
      .orderBy(col("category").desc)
      .show()

    df.groupBy("product").
      agg(sum("amount").as("Total"))
      .orderBy(col("product"))
      .show()

    df.groupBy("category", "product").
      agg(sum("amount").as("Total"))
      .orderBy(col("category"),col("product"))
      .show()

    df.groupBy("product", "category").
      agg(sum("amount").as("Total"))
      .orderBy(col("product"),col("category"))
      .show()
  }
}