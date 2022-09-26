package cmplx

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object flattenUsingStar {
  def main(args: Array[String]): Unit = {
    println("================Started============")
    println

    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val jsondf = spark.read.format("json")
      .option("multiline", "true")
      .load("file:///C:/data/reqapi.json")
    jsondf.show()
    jsondf.printSchema()

    val finaldf = jsondf.select("data.*",
        "page",
        "per_page",
        "support.*",
        "total",
        "total_pages"
        )
    finaldf.show()
    finaldf.printSchema()
  }
}