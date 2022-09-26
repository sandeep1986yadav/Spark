package pck
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object datetodayname {
  def main(args: Array[String]): Unit = {
    println("================Started============")
    println

    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val df = spark.read.format("csv").option("header", "true")
      .load("file:///C:/data/uber.csv")
    println("====================Raw Data=======================")
    df.show()
    println("====================Final Data=======================")

    df.withColumn("date", expr("date_format(to_date(date,'MM/dd/yyyy'),'EE')"))
      .withColumnRenamed("date", "day")
      .groupBy(col("dispatching_base_number"), col("day"))
      .agg(sum("trips").as("sum_trips")).show()

    df.withColumn("date", date_format(to_date(col("date"), "MM/dd/yyyy"),"EE"))
      .withColumnRenamed("date", "day")
      .groupBy(col("dispatching_base_number"), col("day"))
      .agg(sum("trips").as("sum_trips")).show()

  }
}