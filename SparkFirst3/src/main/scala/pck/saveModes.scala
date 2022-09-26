package pck

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object saveModes {
   def main(args : Array[String]):Unit={
		println("================Started============")
		println

		val conf = new SparkConf().setAppName("first").setMaster("local[*]")
		val sc = new SparkContext(conf)
		sc.setLogLevel("ERROR")
		
		val spark = SparkSession.builder().getOrCreate()
		import spark.implicits._
		
		val df = spark.read.format("csv").load("file:///C:/data/datatxns.txt")
		
		//df.write.format("csv").save("file:///C:/data/mdata")
		//df.write.format("csv").mode("append")save("file:///C:/data/mdata")
		//df.write.format("csv").mode("overwrite")save("file:///C:/data/mdata")
		df.write.format("csv").mode("ignore")save("file:///C:/data/mdata")
   }
}