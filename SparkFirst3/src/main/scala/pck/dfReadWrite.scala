package pck

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object dfReadWrite {
	def main(args : Array[String]):Unit={

			val conf = new SparkConf().setAppName("first").setMaster("local[*]")
			val sc = new SparkContext(conf)

			sc.setLogLevel("ERROR")

			val spark = SparkSession.builder().getOrCreate()
			import spark.implicits._
			
			val df = spark.read.format("json")
			          .load("file:///C:/data/devices.csv")
			
			df.show()
			
			df.createOrReplaceTempView("jdf")
			
			val finaldf = spark.sql("select * from jdf where lat>40")
			
			finaldf.write.format("orc").mode("overwrite").save("file:///C:/data/orcw")

	}
}