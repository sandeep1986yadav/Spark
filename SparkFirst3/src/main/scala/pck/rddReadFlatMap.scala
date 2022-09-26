package pck

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object rddReadFlatMap {
	def main(args : Array[String]): Unit={
			println("================Started============")
			println

			val conf = new SparkConf().setAppName("first").setMaster("local[*]")
			val sc = new SparkContext(conf)
			sc.setLogLevel("ERROR")
			println("======== file data=====")
			
			val liststr = sc.textFile("file:///C:/data/bdata.txt", 1)
			liststr.foreach(println)
			    
			println
			println("================flat List============")
			println
			
			val flatdata = liststr.flatMap( x => x.split("-"))
			flatdata.foreach(println)
	}
}