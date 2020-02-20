
import org.apache.spark.{SparkConf, SparkContext}
import java.util.Base64
import java.nio.charset.StandardCharsets
import org.apache.spark.sql.functions.unbase64

object task1 {
  def main(args: Array[String]) = {
    val conf = new SparkConf().
      setMaster("local").
      setAppName("task1")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    //Task 0 - load data into seperate RDDs
    val businessesRDD = sc.textFile("data/yelp_businesses.csv")
    val topReviewersRDD = sc.textFile("data/yelp_top_reviewers_with_reviews.csv").mapPartitionsWithIndex((index, it) => if (index == 0) it.drop(1) else it)
    val topUsersFriendshipRDD = sc.textFile("data/yelp_top_users_friendship_graph.csv")

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
      map(r => new String(Base64.getMimeDecoder().decode(r), StandardCharsets.UTF_8))
    val reviewLength = reviews.map(_.length())
    val avgReviewLength = reviewLength.reduce(_+_) / numReviews

    val top10Business = topReviewersRDD.map(r => (r.split("\t")(businessIDIndex), 1)).
      reduceByKey(_+_).takeOrdered(10)(Ordering.Int.reverse.on(_._2))

    println("Task 2:")
    println("a) Number of distinct users           = " + distinctUsers.count())
    println("b) Average review length              = " + avgReviewLength)
    println("c) Top 10 businesses sorted by reviews: " )
    top10Business.map(v => v._1).foreach(v => println("   - " + v))

    sc.stop()
  }
}