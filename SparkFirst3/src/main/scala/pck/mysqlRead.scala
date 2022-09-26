package pck

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object mysqlRead {
	def main(args : Array[String]):Unit={
			println("================Started============")
			println

			val conf = new SparkConf().setAppName("first").setMaster("local[*]")
			val sc = new SparkContext(conf)
			sc.setLogLevel("ERROR")

			val spark = SparkSession.builder().getOrCreate()
			import spark.implicits._

			val df=   spark.read.format("jdbc")
			.option("url","jdbc:mysql://zeyodb.czvjr3tbbrsb.ap-south-1.rds.amazonaws.com/zeyodb")
			.option("user","root")
			.option("password","Aditya908")
			.option("dbtable","kgf")
			.option("driver","com.mysql.cj.jdbc.Driver")
			.load()

			df.show()


	}

}