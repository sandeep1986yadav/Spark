package pck

object listFilterMap2 {
  def main(args:Array[String]):Unit={
  println("======== Raw List ========")
  val strList = List("zeyobron","analytics","zeyo")
  strList.foreach(println)
  
  println("======== String Filter List ========")
  strList.filter(p => p.contains("zeyo")).foreach(println)
  
  println("======== String Concat List ========")
  strList.map(x => x.concat(",bigdata")).foreach(println)
  
   println("======== String Replace List ========")
  strList.map(x => x.replace("zeyo", "tera")).foreach(println)
  }
}