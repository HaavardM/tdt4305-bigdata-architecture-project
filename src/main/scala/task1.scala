
import org.apache.spark.{SparkConf, SparkContext}
import java.util.{Base64, Date}
import java.nio.charset.StandardCharsets

import org.apache.spark.sql.functions.unbase64
import org.joda.time.DateTime

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
    val businessCount = businessesRDD.count()
    val reviewCount = topReviewersRDD.count()
    val userCount = topUsersFriendshipRDD.count()
    println("Task 1: LineCount")
    println("- businesses    = " + businessCount)
    println("- top_reviewers = " + reviewCount)
    println("- top_users     = " + userCount)

    //Task 2 - explore users
    val distinctUsers = topReviewersRDD.map(_.split('\t')(userIDIndex)).distinct()
    val reviews = topReviewersRDD.map(_.split('\t')(reviewTextIndex)).
      map(r => new String(Base64.getMimeDecoder().decode(r), StandardCharsets.UTF_8))
    val reviewLength = reviews.map(_.length())
    val avgReviewLength = reviewLength.reduce(_+_) / reviewCount

    val top10Business = topReviewersRDD.map(r => (r.split('\t')(businessIDIndex), 1)).
      reduceByKey(_+_).takeOrdered(10)(Ordering.Int.reverse.on(_._2))
    val reviewsUnixTimestamp = topReviewersRDD.
      map(l => l.split('\t')(reviewDateIndex).split('.')(0).toLong)
    val reviewsPerYear = reviewsUnixTimestamp.
      map(l => new DateTime(l*1000).toDateTime().getYear()).
      map(y => (y, 1)).
      reduceByKey(_+_)

    val reviewsMinUnixTimestamp = new DateTime(reviewsUnixTimestamp.min()*1000).toDateTime()
    val reviewsMaxUnixTimestamp = new DateTime(reviewsUnixTimestamp.max()*1000).toDateTime()



    //Create an RDD with (user_id, (1, review_length)) for each review
    val userReviewRDD = topReviewersRDD.map(l => {
      val s = l.split('\t')
      val reviewLength = Base64.getMimeDecoder.decode(s(reviewTextIndex)).length
      (s(userIDIndex), (1, reviewLength))
    })

    //Get the average number of reviews for all users (X_mean)
    val avgReviewsPerUser = reviewCount / userCount

    //Sum the tuples by key and divide the review length sum by the number of reviews.
    //Each element then contains the number of reviews and the average review length for one user
    val reviewsCountLengthPair = userReviewRDD.
      reduceByKey((v1, v2) => (v1._1 + v2._1, v1._2 + v2._2)).
      map(u => (u._2._1, u._2._2 / u._2._1))

    //Calculate the numerator, and denominator
    val numerator = reviewsCountLengthPair.
      map(u => (u._1 - avgReviewsPerUser)*(u._2 - avgReviewLength)).
      reduce(_+_)
    val reviewCountDenominator = math.sqrt(reviewsCountLengthPair.map(k => math.pow(k._1 - avgReviewsPerUser, 2)).reduce(_+_))
    val reviewLengthDenominator = math.sqrt(reviewsCountLengthPair.map(k => math.pow(k._2 - avgReviewLength, 2)).reduce(_+_))
    val denominator = reviewCountDenominator * reviewLengthDenominator
    val pcc = numerator / denominator


    println("Task 2: Top Reviews")
    println("a) Number of distinct users           = " + distinctUsers.count())
    println("b) Average review length              = " + avgReviewLength)
    println("c) Top 10 businesses sorted by reviews: " )
    top10Business.map(v => v._1).foreach(v => println("   - " + v))
    println("d) Number of reviews per year: ")
    reviewsPerYear.sortBy(_._1).foreach(y => printf("   %d: %d\n", y._1, y._2))
    printf("e) First and last review: first = %s, last = %s\n",
      reviewsMinUnixTimestamp.toString("yyyy/MM/dd hh:mm:ss"),
      reviewsMaxUnixTimestamp.toString("yyyy/MM/dd hh:mm:ss"))
    printf("f) Pearson correlation coefficient = %.6f\n", pcc)



    sc.stop()
  }
}