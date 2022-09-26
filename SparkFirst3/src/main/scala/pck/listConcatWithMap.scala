package pck

object listConcatWithMap {
  def main(args:Array[String]):Unit={
    val liststr= List("Bigdata","spark","hive")
    liststr.map(x => "zeyo,"+x).foreach(println)
  }
}