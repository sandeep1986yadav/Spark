package pck

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object filterUsdata {
  def main(args : Array[String]):Unit={
		println("================Started============")
		println

		val conf = new SparkConf().setAppName("first").setMaster("local[*]")
		val sc = new SparkContext(conf)
		sc.setLogLevel("ERROR")
		
		val spark = SparkSession.builder().getOrCreate()
		import spark.implicits._
		
		val df = spark.read.format("csv").option("header", "true")
		.load("file:///C:/data/usdata.csv")
		
		val tdf = df.filter(col("state") === "LA" && col("age") > 10)
		
		tdf.show()
		
  }
}