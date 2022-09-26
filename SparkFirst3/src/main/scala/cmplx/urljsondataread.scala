package cmplx

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.io.Source

object urljsondataread {

  def main(args: Array[String]): Unit = {
    println("================Started============")
    println

    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    for (i <- 0 until 10) {
      // reading from URL as String
      val result = Source.fromURL("https://randomuser.me/api/0.8/?results=10").mkString

      // COnverting steing to rdd[String]
      val rdd = sc.parallelize(List(result))

      val df = spark.read.json(rdd)

      val flattendf = df.select(
        col("nationality"),
        explode(col("results")).as("results"),
        col("seed"),
        col("version"))

      val finaldf = flattendf.select(
        col("nationality"),
        col("results.user.cell"),
        col("results.user.dob"),
        col("results.user.email"),
        col("results.user.gender"),
        col("results.user.location.*"),
        col("results.user.md5"),
        col("results.user.name.*"),
        col("results.user.password"),
        col("results.user.phone"),
        col("results.user.picture.*"),
        col("results.user.registered"),
        col("results.user.salt"),
        col("results.user.sha1"),
        col("results.user.sha256"),
        col("results.user.username"),
        col("seed"),
        col("version")).withColumn("today", current_date())

      finaldf.write.format("csv").
        option("header", "true").
        partitionBy("today").
        mode("append").
        save("file:///C:/data/urldata")
    }
  }

}