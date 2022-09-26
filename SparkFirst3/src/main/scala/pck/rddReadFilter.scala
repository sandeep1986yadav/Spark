package pck

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object rddReadFilter {
   def main(args : Array[String]):Unit={
    println("================Started============")
			println

			val conf = new SparkConf().setAppName("first").setMaster("local[*]")
			val sc = new SparkContext(conf)
			sc.setLogLevel("ERROR")
			println("======== file data=====")
			println
			val data = sc.textFile("file:///C:/data/txns")
			data.foreach(println)
			
			println
			println("======== filer data=====")
			println
			
			val filterdata = data.filter(x => x.contains("Gymnastics"))
			filterdata.foreach(println)
  }
}