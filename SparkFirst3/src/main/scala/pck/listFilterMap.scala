package pck

import org.apache.spark.SparkContext

object listFilterMap {
  def main(args:Array[String]):Unit={
    
    println("=========Raw List=========")
    
    val listin = List(1,2,3,4)
    
    listin.foreach(println)
    
    println("=========Filter List >2=========")
    val listfilter = listin.filter(x => x > 2)
    listfilter.foreach(println)
    
    println("=========Filter List <2=========")
    val listfil = listin.filter(x => x < 2)
    listfil.foreach(println)
    
    println("=========Multiply List *2=========")
    listin.map(x => x*2).foreach(println)
    
    println("=========Complex List *2=========")
    listin.map(x => (x*100)/2).foreach(println)
    
  }
}