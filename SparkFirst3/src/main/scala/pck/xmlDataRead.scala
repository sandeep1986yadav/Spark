package pck

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object xmlDataRead {
  def main(args : Array[String]):Unit={
		println("================Started============")
		println

		val conf = new SparkConf().setAppName("first").setMaster("local[*]")
		val sc = new SparkContext(conf)
		sc.setLogLevel("ERROR")
		
		val spark = SparkSession.builder().getOrCreate()
		import spark.implicits._
		
		val xmldf = spark.read.format("xml").option("rowTag", "book")
		.load("file:///C:/data/book.xml")
		
		xmldf.show()
		
		xmldf.printSchema()
		
		
		val xmldf1 = spark.read.format("xml").option("rowTag", "POSLog")
		.load("file:///C:/data/transactions.xml")
		
		xmldf1.show()
		
		xmldf1.printSchema()
		
		val xmldf2 = spark.read.format("xml").option("rowTag", "Transaction")
		.load("file:///C:/data/multixml")
		
		xmldf2.show()
		
		xmldf2.printSchema()
		
  }
}