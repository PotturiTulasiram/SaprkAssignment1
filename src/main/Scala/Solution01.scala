
import org.apache.spark._
import org.apache.log4j._


object Solution01 extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]","Solution01")
  val lines = sc.textFile("src/main/resources/user.csv")
  val dataWithHeader = lines.first()

  val data = lines.filter(x => x!=dataWithHeader)
  val locations = data.map(x => x.split(",")).map(x => x(3)).distinct()
  var count = 1
  for{
    i <- locations
  }(count = count+1)
  println(count)

}
