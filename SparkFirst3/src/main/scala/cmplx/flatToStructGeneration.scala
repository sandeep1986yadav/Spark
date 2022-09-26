package cmplx

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object flatToStructGeneration {
  def main(args: Array[String]): Unit = {
    println("================Started============")
    println

    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val flatdf = spark.read.format("csv").option("header", "true")
      .load("file:///C:/data/partdata.csv")

    flatdf.show()
    flatdf.printSchema()

    val structData = flatdf.select(
      col("Technology"),
      col("TrainerName"),
      col("id"),
      struct(
        col("permanent"),
        col("temporary"),
        col("workloc")).as("address"))

    structData.show()
    structData.printSchema()

    val structData1 = flatdf.select(
      col("Technology"),
      col("TrainerName"),
      col("id"),
      struct(
        struct(
          col("permanent"),
          col("temporary"),
          col("workloc")).as("user")).as("address"))

    structData1.show()
    structData1.printSchema()

    val structdf = flatdf.withColumn(
      "address",
      struct(col("permanent"), col("temporary"), col("workloc")))
    structdf.show()
    structdf.printSchema()

    structdf.drop("permanent", "temporary", "workloc").show()
  }
}