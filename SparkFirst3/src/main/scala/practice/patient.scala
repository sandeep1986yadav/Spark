package practice

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object patient {
  def main(args: Array[String]): Unit = {
    println("================Started============")
    println

    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val df = spark.read.format("csv").option("header", "true")
      .load("file:///C:/data/practice/Patient.txt")

    df.show

    df.withColumn(
      "illnes_condition",
      when(col("body_temp_fh").between(96f, 97f), "normal")
        .when(col("body_temp_fh").between(97.1f, 99.9f), "initial")
        .when(col("body_temp_fh").between(100f, 100.9f), "modrate")
        .otherwise("serious")).show
  }
}