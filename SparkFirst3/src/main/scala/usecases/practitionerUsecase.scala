package usecases
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.Column

object practitionerUsecase {
  def main(args: Array[String]): Unit = {
    println("================Started============")
    println

    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val df = spark.read.format("csv").option("header", "true")
      .option("delimiter", "~")
      .load("file:///C:/data/PractitionerLatest.txt")

    val df2 = df
      .select(split(col("PROV_PH_DTL"), "\\$").getItem(0).as("TEMP"))

    df2.show()

    val df3 = df2.select(
      split(col("TEMP"), "\\|").getItem(0).as("PH_TY_CD"),
      split(col("TEMP"), "\\|").getItem(1).as("PH_NUM"),
      split(col("TEMP"), "\\|").getItem(2).as("PH_EXT_NUM"),
      split(col("TEMP"), "\\|").getItem(3).as("PH_PRIM_IND"))


    df3.select(replaceEmptyCols(df3.columns):_*).show()

    val list = List("a","b","c")
  }
  
  def replaceEmptyCols(columns: Array[String]): Array[Column] = {
      columns.map(c => {
        when(col(c) === "", null).otherwise(col(c)).alias(c)
      })
    }
}