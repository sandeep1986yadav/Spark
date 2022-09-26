package cmplx
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object generatingArrayType {
  def main(args: Array[String]): Unit = {
    println("================Started============")
    println

    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val df = spark.read.format("json").option("multiline", "true")
      .load("file:///C:/data/pets.json")
    println("====================Raw Data=======================")
    df.show()
    df.printSchema()

    val flatdata = df.select(
      col("Address.*"),
      col("Mobile"),
      col("Name"),
      explode(col("Pets")).as("Pets"),
      col("status"))

    flatdata.show()
    flatdata.printSchema()

    val arraydf = flatdata.groupBy("Permanent address", "current Address",
      "Mobile", "Name", "status")
      .agg(collect_list("Pets").as("Pets"))

    arraydf.show()
    arraydf.printSchema()

    val arrstructdf = arraydf.withColumn(
      "address",
      struct(
        col("Permanent address"),
        col("current Address")))
        .drop("Permanent address","current Address")
        
    arrstructdf.show()
    arrstructdf.printSchema()
  }
}