package cmplx
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object jsonFlatAndRevert {
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
    df.show()
    df.printSchema()

    val flatdf = df.withColumn("data", explode(col("data")))
      .select(
        col("data.*"),
        col("page"),
        col("per_page"),
        col("support.*"),
        col("total"),
        col("total_pages"))

    flatdf.show()
    flatdf.printSchema()

    val finaldf = flatdf.withColumn("support", struct(col("text"), col("url")))
      .drop("text", "url")
      .groupBy("page", "per_page", "support", "total", "total_pages")
      .agg(collect_list(struct(
        col("avatar"),
        col("email"),
        col("first_name"),
        col("id"),
        col("last_name"))).as("data"))
        
        finaldf.show()
        finaldf.printSchema()
  }
}