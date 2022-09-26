package pck
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object seconhighestExp {

  def main(args: Array[String]): Unit = {
    println("================Started============")
    println

    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val df = spark.read.format("csv").option("header", "true")
      .load("file:///C:/data/dtdata.csv")
    println("====================Raw Data=======================")
    df.show()

    val byDeptOrderBySalary = Window
      .partitionBy("dept")
      .orderBy(col("salary").desc)

    val finaldf = df.withColumn(
      "rank",
      dense_rank() over byDeptOrderBySalary)
    
     finaldf.select("dept", "salary")
     .filter(col("rank") === 2)
     .show()
  }

}