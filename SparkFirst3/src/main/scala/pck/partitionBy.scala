package pck

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object partitionBy {
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
		
		usdf.show()
		val finaldf = usdf.filter("age>10")
		
		finaldf.write.format("csv").partitionBy("state").mode("overwrite")
		.save("file:///C:/data/uspart")
		
		finaldf.write.format("csv").partitionBy("state","county")
		.mode("overwrite")
		.save("file:///C:/data/ussubpart")
		
		val df = spark.read.format("csv")
		.load("file:///C:/data/ussubpart")
		
		df.filter("state='CA'").show()
		
  }
}