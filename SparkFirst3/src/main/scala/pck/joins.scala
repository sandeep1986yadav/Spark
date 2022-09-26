package pck

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object joins {
  def main(args: Array[String]): Unit = {
    println("================Started============")
    println

    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val df1 = spark.read.format("csv").option("header", "true")
      .load("file:///C:/data/join1.csv")

    val df2 = spark.read.format("csv").option("header", "true")
      .load("file:///C:/data/join2.csv")

    val df3 = spark.read.format("csv").option("header", "true")
      .load("file:///C:/data/join3.csv")

    // when column name are different in both tables we use as below
    df1.join(df2, df1("txnno") === df2("tno"), "inner")
      .orderBy(col("txnno"))
      .show()

    df1.join(df2, df1("txnno") === df2("tno"), "inner")
      .drop(col("tno"))
      .orderBy(col("txnno"))
      .show()

    df1.join(df2, df1("txnno") === df2("tno"), "left")
      .drop(col("tno"))
      .orderBy(col("txnno"))
      .show()

    df1.join(df2, df1("txnno") === df2("tno"), "right")
      .drop(col("txnno"))
      .orderBy(col("tno"))
      .show()

    df1.join(df2, df1("txnno") === df2("tno"), "full")
      .orderBy(col("txnno"))
      .show()

    df1.join(df2, df1("txnno") === df2("tno"), "full")
      .withColumn("txnno", expr("case when txnno is null then tno else txnno end"))
      .drop(col("tno"))
      .orderBy(col("txnno"))
      .show()

    // when column name are same in both tables we use as below

    df1.join(df3, Seq("txnno"), "inner")
      .orderBy(col("txnno"))
      .show()

    df1.join(df3, Seq("txnno"), "left")
      .orderBy(col("txnno"))
      .show()

    df1.join(df3, Seq("txnno"), "right")
      .orderBy(col("txnno"))
      .show()

    df1.join(df3, Seq("txnno"), "full")
      .orderBy(col("txnno"))
      .show()

    df1.join(df3, Seq("txnno"), "leftsemi")
      .orderBy(col("txnno"))
      .show()

    // left anti join
    val df4 = df2.select("tno")

    df1.join(df4, df1("txnno") === df4("tno"), "left_anti")
      .show()

    df1.join(df2, df1("txnno") === df2("tno"), "full")
      .withColumn("txnno", expr("case when txnno is null then tno else txnno end"))
      .drop(col("tno"))
      .orderBy(col("txnno"))
      .show()

  }
}