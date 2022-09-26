package pck

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object specificClmSlct {
  def main(args : Array[String]):Unit={
		println("================Started============")
		println

		val conf = new SparkConf().setAppName("first").setMaster("local[*]")
		val sc = new SparkContext(conf)
		sc.setLogLevel("ERROR")
		
		val spark = SparkSession.builder().getOrCreate()
		import spark.implicits._
		
		val usdf = spark.read.format("csv").option("header", "true")
		.load("file:///C:/data/usdata.csv")
		
		usdf.select("first_name", "last_name").show()
		
		usdf.select(col("first_name"), col("last_name")).show()

  }
}