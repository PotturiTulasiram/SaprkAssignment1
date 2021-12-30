
import org.apache.spark._
import org.apache.log4j._

object Solution03 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def tupleData(line:String):(Int,Int) = {
    val fields = line.split(",")
    val userId = fields(2).toInt
    val price = fields(3).toInt

    (userId,price)
  }
  val sc = new SparkContext("local[*]","Solution03")
  val lines = sc.textFile("src/main/resources/transactions.csv")

  val rdd = lines.map(tupleData)
  val reducedRdd = rdd.reduceByKey((x,y) => x+y).sortByKey(true)

  reducedRdd.collect().foreach(println)

}
