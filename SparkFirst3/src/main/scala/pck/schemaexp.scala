package pck

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object schemaexp {
  
case class tschema(id:String,tdate:String,category:String,product:String)

def main(args : Array[String]):Unit={
		println("================Started============")
		println

		val conf = new SparkConf().setAppName("first").setMaster("local[*]")
		val sc = new SparkContext(conf)
		sc.setLogLevel("ERROR")
		
		val spark = SparkSession.builder().getOrCreate()
		import spark.implicits._
		
		println("======== file data=====")
		println
		val data = sc.textFile("file:///C:/data/datatxns.txt")
		data.foreach(println)

		println
		println("======== column filer data=====")
		println
		val finaldata = data.map(x => x.split(","))
		.map(x => tschema(x(0),x(1),x(2),x(3)))
		.filter(x => x.product.contains("Gymnastics"))
		
		finaldata.foreach(println)
		println
		
		val df = finaldata.toDF()
		df.show()
		df.coalesce(1).write.parquet("file:///C:/data/dfparque")
}
}