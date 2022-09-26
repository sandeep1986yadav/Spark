package pck

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object avroRead {
	def main(args : Array[String]):Unit={

			val conf = new SparkConf().setAppName("first").setMaster("local[*]")

			val sc = new SparkContext(conf)

			sc.setLogLevel("ERROR")

			val spark = SparkSession.builder().getOrCreate()
			import spark.implicits._

			val df = spark.read.format("avro").load("file:///C:/data/data.avro")

			df.show()
			
			df.printSchema()

	}
}