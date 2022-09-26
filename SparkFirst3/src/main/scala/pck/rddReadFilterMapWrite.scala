package pck
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object rddReadFilterMapWrite {
  def main(args : Array[String]) :Unit ={
    println("================Started============")
			println

			val conf = new SparkConf().setAppName("first").setMaster("local[*]")
			val sc = new SparkContext(conf)
			sc.setLogLevel("ERROR")
			println("======== file data=====")
			println
			val data = sc.textFile("file:///C:/data/usdata.csv")
			data.take(5).foreach(println)
			
			println
			println("======== filter data=====")
			println
			
			val filterdata = data.filter(x => x.length()>200)
			filterdata.foreach(println)
			println
			println("======== flatten data=====")
			println
			val flatdata = filterdata.flatMap(x => x.split(","))
			flatdata.foreach(println)
			println
			println("======== Replaced data=====")
			println
			val replacedata  = flatdata.map(x => x.replace("-", ""))
			replacedata.foreach(println)
			println
			println("======== Final data=====")
			println
			val finaldata =  replacedata.map(x => x.concat(",zeyo"))
      finaldata.foreach(println)
      
      finaldata.coalesce(1).saveAsTextFile("file:///C:/data/firstwrite")
  }
}