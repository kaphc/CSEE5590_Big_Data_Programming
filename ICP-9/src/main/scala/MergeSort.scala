import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object MergeSort {

  System.setProperty("hadoop.home.dir", "D:\\winutils")

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Merge").setMaster("local[*]")
    val sc = new SparkContext(conf)
    def mergeSort(xs: List[Int]): List[Int] = {

      val n = xs.length / 2
      if (n == 0) xs
      else {
        def merge(xs: List[Int], ys: List[Int]): List[Int] =
          (xs, ys) match {
            case(Nil, ys) => ys
            case(xs, Nil) => xs
            case(x :: xs1, y :: ys1) =>
              if (x < y) x::merge(xs1, ys)
              else y :: merge(xs, ys1)
          }

        val (left, right) = xs splitAt(n)
        println(left,right)
        merge(mergeSort(left), mergeSort(right))

      }
    }

    val list = List(38,27,43,3,9,82,10)


    val result = sc.parallelize(Seq(list)).map(mergeSort)
    println(result)
    result.foreach(println)
  }
}