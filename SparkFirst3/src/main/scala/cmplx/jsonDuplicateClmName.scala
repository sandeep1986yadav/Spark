package cmplx

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object jsonDuplicateClmName {
  def main(args: Array[String]): Unit = {
    println("================Started============")
    println

    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    println("========= json with duplicate column in diffrent struct Read=========")
    println
    println

    val plainjsondf = spark.read.format("json")
      .option("multiline", "true")
      .load("file:///C:/data/picturem.json")
    plainjsondf.show()
    plainjsondf.printSchema()

    val finaldf = plainjsondf.selectExpr(
      "id",
      "image.height as image_height",
      "image.url as image_url",
      "image.width as image_width",
      "name",
      "thumbnail.height as thumbnail_height",
      "thumbnail.url as thumbnail_url",
      "thumbnail.width as thumbnail_width",
      "type")

    finaldf.show()
    finaldf.printSchema()

    // Another Way withColumn

    val finaldf1 = plainjsondf
      .withColumn("image_height", col("image.height"))
      .withColumn("image_url", expr("image.url"))
      .withColumn("image_width", expr("image.width"))
      .withColumn("thumbnail_height", expr("thumbnail.height"))
      .withColumn("thumbnail_url", expr("thumbnail.url"))
      .withColumn("thumbnail_width", expr("thumbnail.width"))
      .drop("image", "thumbnail")

    finaldf1.show()
    finaldf1.printSchema()

    //create complex again

    val complxdf = finaldf.select(
      col("id"),
      struct(
        col("image_height").as("height"),
        col("image_url").as("url"),
        col("image_width").as("width")).as("image"),
      col("name"),
      struct(
        col("thumbnail_height").as("height"),
        col("thumbnail_url").as("url"),
        col("thumbnail_width").as("width")).as("thumbnail"),
      col("type"))

    complxdf.show()
    complxdf.printSchema()

  }
}