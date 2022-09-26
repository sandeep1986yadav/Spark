package pck

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType

object dataframeRead {
	def main(args : Array[String]):Unit={
			val conf = new SparkConf().setAppName("first").setMaster("local[*]")
					val sc = new SparkContext(conf)

					sc.setLogLevel("ERROR")

					val spark = SparkSession.builder().getOrCreate()
					import spark.implicits._

					val structSchema = StructType(Array(
							StructField("id",StringType,true),
							StructField("tdate",StringType,true),
							StructField("category",StringType,true),
							StructField("product",StringType,true)))

					val df = spark.read.format("csv").schema(structSchema)
					.load("file:///C:/data/datatxns.txt")

					df.show()

					val df1 = spark.read.format("parquet").
					load("file:///C:/data/data.parquet")

					df1.show()

					val df2 = spark.read.format("json").
					load("file:///C:/data/devices.csv")

					df2.show()

					val df3 = spark.read.format("orc").
					load("file:///C:/data/data.orc")

					df3.show()
	}
}