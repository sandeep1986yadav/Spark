package pck

object listFlatMapReplace {
   def main(args:Array[String]):Unit={
     val data = List("Amazon-Jeff-America","Microsoft-BillGates-America",
         "TCS-TATA-india","Relience-Ambani-INDIA")
     data.filter(x => x.toLowerCase().contains("india"))
     .flatMap(y => y.split("-"))
     .map(z => z.replace("india","local"))
     .map(i => i.concat(",Done"))
     .foreach(println)
   }
}