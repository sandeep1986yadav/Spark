package pck

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object dslExpressions {
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

    df.selectExpr("id", "split(tdate,'-')[2] as year").show()

    df.selectExpr("id", "product", "amount*2 as amt", "nvl(category,'Sandeep') as category").show()

    df.select(col("amount").*(2).name("amount_2")).show()

    df.select(col("tdate").alias("date")).show()

    df.selectExpr(
      "id",
      "split(tdate,'-')[2] as year",
      "amount",
      "category",
      "product",
      "spendby").show()

    df.selectExpr(
      "id",
      "split(tdate,'-')[2] as year",
      "amount",
      "category",
      "product",
      "spendby",
      "case when spendby ='cash' then 0 else 1 end as status")
      .show()

   

  }
}