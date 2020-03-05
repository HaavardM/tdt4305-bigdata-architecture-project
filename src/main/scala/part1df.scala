

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession


object part1df {


  
  def main(args: Array[String]) = {
    val spark = SparkSession
      .builder()
      .appName("Task1df")
      .master("local")
      .getOrCreate()

    //Part 1: Load dataframes
    val businessesDF =  spark.read
      .format("csv")
      .option("header", "true")
      .option("delimiter", "\t")
      .load("data/yelp_businesses.csv")

    val topReviewsDF =  spark.read
      .format("csv")
      .option("header", "true")
      .option("delimiter", "\t")
      .load("data/yelp_top_reviewers_with_reviews.csv")

    val topUsersFriendshipDF =  spark.read
      .format("csv")
      .option("header", "true")
      .option("delimiter", ",")
      .load("data/yelp_top_users_friendship_graph.csv")

    topReviewsDF.show()

    //Part 2:
    //a)
    val joinTable = topReviewsDF.join(businessesDF, topReviewsDF("business_id") === businessesDF("business_id"))
    topReviewsDF.groupBy("user_id").count().sort(desc("count")).show(20)
    spark.stop()

  }
}