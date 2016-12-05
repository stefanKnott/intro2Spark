import org.apache.spark._

object sparkWc {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Wc")
      .setMaster("local")
      
    val sc = new SparkContext(conf)
      
      val file2Count = sc.textFile("/countMe.txt");
      
      file2Count.flatMap { line =>
        line.split(" ")
  }
    .map{ word =>
        (word, 1)
    }
      .reduceByKey(_ + _)
       .saveAsTextFile("output/")
   
    sc.stop()
  }
}