package pck

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._


object rowrddexp {
  def main(args : Array[String]):Unit={
    
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
		println("======== row rdd filtered data=====")
		println
		
		val rowrdd = data.map(x => x.split(",")).map(x => Row(x(0),x(1),x(2),x(3)))
		.filter(x => x(3).toString().contains("Gymnastics"))
    rowrdd.foreach(println)
		
    val structSchema = StructType(Array(
        StructField("id",StringType,true),
        StructField("tdate",StringType,true),
        StructField("category",StringType,true),
        StructField("product",StringType,true)))
		
    var df = spark.createDataFrame(rowrdd, structSchema)  
    
    println
    
    df.show()
    
    df.createOrReplaceTempView("exercise")
    val finaldf = spark.sql("select * from exercise where tdate = '06-26-2011'")
    finaldf.show()    
    
  }
}