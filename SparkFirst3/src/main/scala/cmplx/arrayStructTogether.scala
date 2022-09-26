package cmplx

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object arrayStructTogether {
  def main(args: Array[String]): Unit = {
    println("================Started============")
    println

    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val df = spark.read.format("json").option("multiline", "true")
      .load("file:///C:/data/reqres.json")
    println("====================Raw Data=======================")
    df.show()
    df.printSchema()

    val flatdata = df.withColumn("data", explode(col("data")))
      .withColumn("text", col("support.text"))
      .withColumn("url", col("support.url"))
      .drop("support")

    flatdata.show()
    flatdata.printSchema()

    val finaldf = flatdata.withColumn("avtar", col("data.avatar"))
      .withColumn("email", col("data.email"))
      .withColumn("first_name", col("data.first_name"))
      .withColumn("id", col("data.id"))
      .withColumn("last_name", col("data.last_name"))
      .drop("data")

    finaldf.show()
    finaldf.printSchema()
  }
}