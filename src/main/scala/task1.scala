
import org.apache.spark.{SparkConf, SparkContext}
import java.util.Base64
import java.nio.charset.StandardCharsets

object task1 {
  def main(args: Array[String]) = {
    val conf = new SparkConf().
      setMaster("local").
      setAppName("task1")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    //Task 0 - load data into seperate RDDs
    val businessesRDD = sc.textFile("data/yelp_businesses.csv").mapPartitionsWithIndex((
      (idx, iter) => if (idx == 0) iter.drop(1) else iter)
    )
    val topReviewersRDD = sc.textFile("data/yelp_top_reviewers_with_reviews.csv").mapPartitionsWithIndex((
      (idx, iter) => if (idx == 0) iter.drop(1) else iter)
    )
    val topUsersFriendshipRDD = sc.textFile("data/yelp_top_users_friendship_graph.csv").mapPartitionsWithIndex((
      (idx, iter) => if (idx == 0) iter.drop(1) else iter)
    )

    val reviewIDIndex = 0
    val userIDIndex = 1
    val businessIDIndex = 2
    val reviewTextIndex = 3
    val reviewDateIndex = 4

    //Task 1 - count line numbers for each RDD
    val numReviews = topReviewersRDD.count()
    println("Task 1: LineCount")
    println("- businesses    = " + businessesRDD.count())
    println("- top_reviewers = " + numReviews)
    println("- top_users     = " + topUsersFriendshipRDD.count())

    //Task 2 - explore users
    var distinctUsers = topReviewersRDD.map(_.split("\t")(userIDIndex)).distinct()
    val reviews = topReviewersRDD.map(_.split("\t")(reviewTextIndex)).
      map(r => new String(Base64.getDecoder().decode(r), StandardCharsets.UTF_8)).persist()
    reviews.take(5).foreach(println)
    val reviewLength = reviews.map(_.length())
    val avgReviewLength = reviewLength.reduce(_+_) / numReviews

    println("Task 2:")
    println("a) Number of distinct users = " + distinctUsers.count())
    println("b) Average review length    = " + avgReviewLength)
    sc.stop()
  }
}