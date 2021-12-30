
import org.apache.spark._
import org.apache.log4j._


object Solution02 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def tupledata(line:String):(Int,String) = {
    val fields = line.split(",")
    val userID = fields(2).toInt
    val productId = fields(1)
    (userID,productId)
  }

  val sc = new SparkContext("local[*]","Solution02")

  val lines = sc.textFile("src/main/resources/transactions.csv")
  val rdd = lines.map(tupledata)
  val reducedRdd = rdd.reduceByKey((x,y) => (x+" "+y) ).sortByKey(true)
  reducedRdd.collect().foreach(println)
}
