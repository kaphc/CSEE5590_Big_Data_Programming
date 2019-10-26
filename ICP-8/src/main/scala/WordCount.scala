/**
  * Illustrates flatMap + countByValue for wordcount.
  */


import org.apache.spark._

object WordCount {
  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir", "D:\\winutils")

    //val inputFile = args(0)
    //val outputFile = args(1)
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[*]")

    // Create a Scala Spark Context.
    val sc = new SparkContext(conf)

    // Load our input data.
    //val input =  sc.textFile(inputFile)
    val input = sc.textFile("data/sample.csv")

    // Split up into words.
    val words = input.flatMap(line => line.split(" ").filter(word => word.matches("#[a-z]*")))

    // Transform into word and count.
    val counts = words.map(word => (word, 1)).reduceByKey { case (x, y) => x + y }.sortByKey()

    // Save the word count back out to a text file, causing evaluation.
    counts.saveAsTextFile("output")
  }
}