package pck

object Obj {
  def main(args:Array[String]):Unit={
    val listin = List(1,2,3,4)
    println("=========Multiply List *2=========")
    listin.map(x => x*2).foreach(println)
    
    println("=========Add List *2=========")
    listin.map(x => x+100).foreach(println)
  }
}