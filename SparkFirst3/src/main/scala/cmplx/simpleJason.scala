package cmplx
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object simpleJason {

  def main(args: Array[String]): Unit = {
    println("================Started============")
    println

    val conf = new SparkConf().setAppName("first").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    println("==========Plain json Read=========")
    println
    println
    println

    val plainjsondf = spark.read.format("json")
      .load("file:///C:/data/cm1.json")
    plainjsondf.show()
    plainjsondf.printSchema()

    println("==========Multiline json Read=========")
    println
    println
    println

    val multilinejsondf = spark.read.format("json")
      .option("multiline", "true")
      .load("file:///C:/data/cm2.json")

    multilinejsondf.show()
    multilinejsondf.printSchema()

    println("==========Struct json Read with 1 level hierarchy =========")
    println
    println
    val jsondf = spark.read.format("json")
      .option("multiline", "true")
      .load("file:///C:/data/cm.json")

    jsondf.show()
    jsondf.printSchema()
    println("==========Flattened Struct json Read with 1 level hierarchy =========")
    println
    println
    val fljsondf = jsondf.select(
      "Technology",
      "TrainerName",
      "address.permanent",
      "address.temporary",
      "id")
    fljsondf.show()
    fljsondf.printSchema()

    println("==========Struct json Read with 2 level hierarchy(struct inside struct) =========")
    println
    println

    val jsondf2 = spark.read.format("json")
      .option("multiline", "true")
      .load("file:///C:/data/cm3.json")

    jsondf2.show()
    jsondf2.printSchema()

    println("==========Flattened Struct json Read with 2 level hierarchy(struct inside struct) =========")
    println
    println

    val fljsondf2 = jsondf2.select(
      "Technology",
      "TrainerName",
      "address.user.permanent",
      "address.user.temporary",
      "id")
    fljsondf2.show()
    fljsondf2.printSchema()

    val fljsondf4 = jsondf2.select(
      "Technology",
      "TrainerName",
      "address.user.*", // this will give all the level inside user
      "id")
    fljsondf4.show()
    fljsondf4.printSchema()

    // Another way
    val fljsondf3 = jsondf2.withColumn("permanent", expr("address.user.permanent"))
      .withColumn("temporary", expr("address.user.temporary"))
      .drop("address")
    fljsondf3.show()
    fljsondf3.printSchema()
  }

}