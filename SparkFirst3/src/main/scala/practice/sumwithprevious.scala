package practice

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.Column

object sumwithprevious {
  def main(args: Array[String]): Unit = {
    println("================Started============")
    println

    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val df = spark.read.format("csv").option("header", "true")
      .load("file:///C:/data/practice/itemdata.txt")

    df.show()

    val window = Window.partitionBy("item").orderBy(col("id").desc)
    .rowsBetween(Long.MinValue, 0)

    val i = df.withColumn("bill", sum("price").over(window)).show

  }
}