package pck

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object dslstarts {
   def main(args : Array[String]):Unit={
		println("================Started============")
		println

		val conf = new SparkConf().setAppName("first").setMaster("local[*]")
		val sc = new SparkContext(conf)
		sc.setLogLevel("ERROR")
		
		val spark = SparkSession.builder().getOrCreate()
		import spark.implicits._
		
		val df = spark.read.format("csv").option("header", "true")
		.load("file:///C:/data/dt.txt")
		println("====================Raw Data=======================")
		df.show()
		
		println("====================Select=======================")
		df.select("id", "product").show()
		df.select(col("id"),col("product")).show()
		
		println("====================Filter=======================")
		df.filter("category = 'Gymnastics'").show()
		df.filter(col("category") ==="Gymnastics").show()
		df.filter(col("category").===("Gymnastics")).show()
		
		println("====================Multi Column Filter=======================")
		df.filter(col("category") ==="Gymnastics"
		    && 
		    col("spendby") === "cash"
		).show()
		
		df.filter(col("category") ==="Gymnastics"
		    || 
		    col("spendby") === "cash"
		).show()
		
		println("====================Filter & Select=======================")
		df.filter(col("category") ==="Gymnastics"
		    || 
		    col("spendby") === "cash"
		).select("id", "product","amount").show()
		
		println("====================Like Operator=======================")
		df.filter(col("product") like "%Gymnastics%").show()
		df.filter(col("product").like("%Gymnastics%")).show()
		
		println("====================Multi Value Filter=======================")
		df.filter(col("category") isin ("Gymnastics","Exercise")).show()
		df.filter( col("category").isin("Gymnastics","Exercise")).show()
		
		println("====================not Filter=======================")
		df.filter(col("category") =!= "Gymnastics").show()
		
		df.filter( !(col("category") === "Gymnastics")).show()
		
		df.filter( !(col("product") like "%Gymnastics%")).show()
		
		df.filter( !(col("category") isin ("Gymnastics","Exercise"))).show()
		
		df.filter( !(col("product") like "%Gymnastics%")
		           &&
		           col("spendby") === "cash"
		          ).show()
		
		println("====================Null Filters=======================")
		df.filter( col("id").isNull).show()
		
		df.filter( !(col("id").isNull)).show()
			
		
   }
}