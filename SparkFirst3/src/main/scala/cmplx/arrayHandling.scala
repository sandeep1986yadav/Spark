package cmplx
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object arrayHandling {
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
      
      // another way
      
      val finaldf = df.withColumn("Pets", explode(col("Pets")))
      .withColumn("Permanent address", expr("Address.`Permanent address`"))
      .withColumn("current Address", expr("Address.`current Address`"))
      .drop("Address")
      
      finaldf.show()
      finaldf.printSchema()
  }
}