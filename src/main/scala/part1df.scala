

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
    //Join reviews and businesses table by business_id
    val joinTable = topReviewsDF.join(businessesDF, Seq("business_id"))
    joinTable.show(5)
    //joinTable.coalesce(1).write.csv("results/join_dataframe.csv")
    //Find the top 20 most active users
    topReviewsDF.groupBy("user_id").count().sort(desc("count")).show(20)
    spark.stop()

  }
}