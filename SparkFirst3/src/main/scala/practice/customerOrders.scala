package practice

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object customerOrders {
  def main(args: Array[String]): Unit = {
    println("================Started============")
    println

    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val custdf = spark.read.format("csv").option("header", "true")
      .load("file:///C:/data/practice/customer.txt")

    val orderdf = spark.read.format("csv").option("header", "true")
      .load("file:///C:/data/practice/orders.txt")

    //Calculate total order amount for each customer and display Name,Age,Salary and TotalOrderAmt.
    val sumdf = orderdf.groupBy("Customer_ID").agg(sum("Amount").as("TotalOrderAmount"))

    custdf.join(sumdf, custdf("ID")
      === sumdf("Customer_ID"), "left_outer")
      .select("Name", "Age", "Salary", "TotalOrderAmount").show

    //3. Display all the customers who has placed at least an order .
    custdf.join(orderdf, custdf("ID")
      === orderdf("Customer_ID"), "leftsemi").show()

    val orderedsalary = Window.orderBy(col("salary").desc)

    //4. Find the 2nd highest salary holder customer(Pref using window function)
    custdf.withColumn("rank", dense_rank().over(orderedsalary))
      .filter(col("rank") === 2).show

    //5. Display those customers who has not placed any order yet.
    custdf.join(orderdf, custdf("ID")
      === orderdf("Customer_ID"), "leftanti").show()

  }
}