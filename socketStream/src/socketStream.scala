/**
  * Created by stefanknott on 12/14/16.
  */
import org.apache.spark._
import org.apache.spark.streaming._

object socketStream {
  def isServerError(line: String): Boolean=line.matches("status:5\\d{2}")

  def main(args: Array[String]): Unit ={
    val conf = new SparkConf().setAppName("Error Counter").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(5))
    val lines = ssc.socketTextStream("localhost", 9999)
    val words = lines.flatMap(_.split(" "))

    val serverResult = words.map(word => isServerError(word))

    serverResult.filter((b: Boolean) => b == true).print()

    ssc.start()
    ssc.awaitTermination()
  }
}