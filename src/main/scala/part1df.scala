

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

    //Task 5: Load dataframes
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

    //Task 6:
    //a)
    //Join reviews and businesses table by business_id
    val joinTable = topReviewsDF.join(businessesDF, Seq("business_id"))
    //joinTable.limit(100).coalesce(1).write.csv("results/task6a.csv")
    //b)
    //Store as a temporary table
    joinTable.createOrReplaceTempView("task6b")
    spark.table("task6b").show(10)
    //c)
    //Find the top 20 most active users
    val top20 = topReviewsDF.groupBy("user_id").count().sort(desc("count")).limit(20)
    top20.show(20)
    //top20.coalesce(1).write.csv("results/task6c.csv")
    spark.stop()

  }
}