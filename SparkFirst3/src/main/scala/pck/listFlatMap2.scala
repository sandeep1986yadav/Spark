package pck

object listFlatMap2 {
  
  def main(args:Array[String]):Unit={
    val splitList = List("zeyo~analytics","bigdata~spark","hive~sqoop")
    splitList.flatMap(x => x.split("~")).foreach(println)
   
  }

}
