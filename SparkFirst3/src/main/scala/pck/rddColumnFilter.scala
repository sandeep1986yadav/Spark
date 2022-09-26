package pck

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext


object rddColumnFilter {
  def main(args : Array[String]):Unit={
    println("================Started============")
			println

			val conf = new SparkConf().setAppName("first").setMaster("local[*]")
			val sc = new SparkContext(conf)
			sc.setLogLevel("ERROR")
			println("======== file data=====")
			println
			val data = sc.textFile("file:///C:/data/datatxns.txt")
			data.foreach(println)
			
			println
			println("======== filer data=====")
			println
			
			val filterdata = data.filter(x => x.contains("Gymnastics"))
			filterdata.foreach(println)
			
			println
			println("======== column filer data=====")
			println
			
			data.map(x => x.split(","))
			.filter(i => i(3).contains("Gymnastics")).
			foreach(x => println(x(0)+","+x(1)+","+x(2)+","+x(3)))
  }
}