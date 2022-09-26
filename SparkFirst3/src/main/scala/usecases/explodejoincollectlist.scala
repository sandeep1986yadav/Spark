package usecases

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object explodejoincollectlist {
  def main(args: Array[String]): Unit = {
    println("================Started============")
    println

    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val empdf = spark.read.format("csv").option("header", "true")
      .option("delimiter", "~")
      .load("file:///C:/data/sqlusecase/file1.csv")

    empdf.show()
    println
    println
    val mgrdf = spark.read.format("csv").option("header", "true")
      .option("delimiter", "~")
      .load("file:///C:/data/sqlusecase/file2.csv")
    println("====================Raw Data=======================")

    mgrdf.show()
    println
    println

    val flattenEmpDf = empdf.withColumn(
      "empman",
      explode(split(col("empman"), ",").as("empman")))

    println("================Final Data DSL===========")
    flattenEmpDf.join(mgrdf, mgrdf("mno") === flattenEmpDf("empman"), "inner")
      .groupBy("empno", "empname")
      .agg(collect_list("mname").as("mname"))
      .withColumn("mname", concat_ws(",", col("mname")))
      .orderBy(col("empno"))
      .show()

    empdf.createOrReplaceTempView("emp")
    mgrdf.createOrReplaceTempView("mno")

    println("================Final Data SQL===========")
    spark.sql("select empno,empname,concat_ws(',',collect_list(mname)) as mname " +
      "from (select empt1.*, mno.mname from mno " +
      "join (select emp.*,explode(split(emp.empman,',')) as " +
      "     e_mno from emp) empt1 " +
      "on mno.mno = empt1.e_mno " +
      ")group by empno, empname " +
      "Order by empno")
      .show()

  }
}