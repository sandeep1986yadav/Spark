package pck

object listFlatMap3 {
  def main(args:Array[String]):Unit={
    val lisstr1 = List(  
		"State->TN~City->Chennai"  ,     
		"State->Gujarat~City->GandhiNagar"
		)
		
		lisstr1.flatMap(x => x.split("~"))
		.flatMap(y => y.split("->"))
		.foreach(println)
  }
}