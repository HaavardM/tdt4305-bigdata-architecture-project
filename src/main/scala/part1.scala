
import org.apache.spark.{SparkConf, SparkContext}
import java.util.{Base64}
import java.nio.charset.StandardCharsets

import org.joda.time.DateTime

object part1 {

  val ReviewsIDIndex = 0
  val ReviewsUserIndex = 1
  val ReviewsBusinessIDIndex = 2
  val ReviewsTextIndex = 3
  val ReviewsDateIndex = 4

  val BusinessIDIndex = 0
  val BusinessNameIndex = 1
  val BusinessAddressIndex = 2
  val BusinessCityIndex = 3
  val BusinessStateIndex = 4
  val BusinessPostalCodeIndex = 5
  val BusinessLatitudeIndex = 6
  val BusinessLongitudeIndex = 7
  val BusinessStarsIndex = 8
  val BusinessReviewCountIndex = 9
  val BusinessCategoriesIndex = 10

  val FriendshipSourceIndex = 0
  val FriendshipDestionationIndex = 1
  
  def main(args: Array[String]) = {
    val conf = new SparkConf().
      setMaster("local").
      setAppName("task1")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    //Task 0 - load data into seperate RDDs and drop first line (description)
    val businessesRDD = sc.textFile("data/yelp_businesses.csv").mapPartitionsWithIndex((index, it) => if (index == 0) it.drop(1) else it)
    val topReviewersRDD = sc.textFile("data/yelp_top_reviewers_with_reviews.csv").mapPartitionsWithIndex((index, it) => if (index == 0) it.drop(1) else it)
    val topUsersFriendshipRDD = sc.textFile("data/yelp_top_users_friendship_graph.csv").mapPartitionsWithIndex((index, it) => if (index == 0) it.drop(1) else it) 

    //Task 1 - count line numbers for each RDD
    val businessCount = businessesRDD.count()
    val reviewCount = topReviewersRDD.count()
    val userCount = topUsersFriendshipRDD.count()
    //Task 2 - explore reviews
    //A) Find distinct users
    val distinctUserCount = topReviewersRDD.map(_.split('\t')(ReviewsUserIndex)).distinct().count()
    //B) Find the average number of characters in a review
    val reviews = topReviewersRDD.map(_.split('\t')(ReviewsTextIndex)).
      map(r => new String(Base64.getMimeDecoder().decode(r), StandardCharsets.UTF_8))
    val avgReviewLength = reviews.map(_.length()).reduce(_+_) / reviewCount
    //C) Find the top 10 businesses with most reviews
    val top10Business = topReviewersRDD.map(r => (r.split('\t')(ReviewsBusinessIDIndex), 1)).
      reduceByKey(_+_).takeOrdered(10)(Ordering.Int.reverse.on(_._2))
    //D) Find the number of reviews per year
    val reviewsUnixTimestamp = topReviewersRDD.
      map(l => l.split('\t')(ReviewsDateIndex).split('.')(0).toLong)
    val reviewsPerYear = reviewsUnixTimestamp.
      map(l => new DateTime(l*1000).toDateTime().getYear()).
      map(y => (y, 1)).
      reduceByKey(_+_)
    val reviewsMinTimestamp = new DateTime(reviewsUnixTimestamp.min()*1000).toDateTime()
    val reviewsMaxTimestamp = new DateTime(reviewsUnixTimestamp.max()*1000).toDateTime()
    reviewsPerYear.saveAsTextFile("results/reviewsPerYear.csv")

    //E) Calculate the Pearson Correlation Coefficient
    //Create an RDD with (user_id, (1, review_length)) for each review
    val userReviewRDD = topReviewersRDD.map(l => {
      val lineSplit = l.split('\t')
      val reviewLength = Base64.getMimeDecoder.decode(lineSplit(ReviewsTextIndex)).length
      //User ID => (reviewCount, reviewLengthSum)
      (lineSplit(ReviewsUserIndex), (1, reviewLength))
    })

    //Get the average number of reviews for all users (X_mean)
    val avgReviewsPerUser = reviewCount / userCount

    //Sum the tuples by key and divide the review length sum by the number of reviews.
    //Each element then contains the number of reviews and the average review length for one user
    val reviewsCountLengthPair = userReviewRDD.
      reduceByKey((first, second) => (first._1 + second._1, first._2 + second._2)).
      map(u => {
        val reviewCount = u._2._1
        val reviewLength = u._2._2
        (reviewCount, reviewLength / reviewCount)
      })

    //Calculate the numerator, and denominator
    val pccNumerator = reviewsCountLengthPair.
      map(u => (u._1 - avgReviewsPerUser)*(u._2 - avgReviewLength)).
        reduce(_+_)
    val pccReviewCountDenominator = math.sqrt(reviewsCountLengthPair.
      map(k => math.pow(k._1 - avgReviewsPerUser, 2)).reduce(_+_))
    val pccReviewLengthDenominator = math.sqrt(reviewsCountLengthPair.
      map(k => math.pow(k._2 - avgReviewLength, 2)).reduce(_+_))
    val pccDenominator = pccReviewCountDenominator * pccReviewLengthDenominator
    val pcc = pccNumerator / pccDenominator


    //Task 3: Businesses
    //A) Average business rating
    val avgBusinessRatingByCity = businessesRDD.map(l => {
      val lineSplit = l.split('\t')
      // CityID => (BusinessCount, StarsSum)
      (lineSplit(BusinessCityIndex), (1, lineSplit(BusinessStarsIndex).toFloat))
    }).reduceByKey((first, second) => (first._1 + second._1, first._2 + second._2)).
      map(city => (city._1, city._2._2 / city._2._1))
    //B) Top 10 most frequent categories
    val top10BusinessCategories = businessesRDD.
      map(l => (l.split('\t')(BusinessCategoriesIndex), 1)).
      reduceByKey(_+_).
      takeOrdered(10)(Ordering.Int.reverse.on(_._2)).
      map(_._1)
    //C) Calculate the postal code centroid
    val businessPostalCodeCentroid = businessesRDD.
      map(l => {
        val lineSplit = l.split('\t')
        //PostalCode => (Count, LatSum, LongSum)
        (lineSplit(BusinessPostalCodeIndex),
          (1,
            lineSplit(BusinessLatitudeIndex).toFloat,
            lineSplit(BusinessLongitudeIndex).toFloat))
      }).reduceByKey((first, second) => {
      (first._1 + second._1,
        first._2 + second._2,
        first._3 + second._3)
      }).map(p => {
      val postalCode = p._1
      val businesses = p._2._1
      val latitudeSum = p._2._2
      val longitudeSum = p._2._3
      (postalCode, (latitudeSum / businesses, longitudeSum / businesses))
    })

    //Task 4: Friendship graph
    //A) Find top 10 most active nodes
    val friendshipCount = topUsersFriendshipRDD.flatMap(l => {
      val lineSplit = l.split(',')
      Array(
        //UserID => (OutCount, InCount)
        (lineSplit(FriendshipSourceIndex), (1, 0)),
        (lineSplit(FriendshipDestionationIndex), (0, 1))
      )
    }).reduceByKey((first, second) => {
      (first._1 + second._1, first._2 + second._2)
    })
    friendshipCount.saveAsTextFile("results/friendship_count.csv")

    val top10InFriendships = friendshipCount.takeOrdered(10)(Ordering.Int.reverse.on(_._2._2))
    val top10OutFriendships = friendshipCount.takeOrdered(10)(Ordering.Int.reverse.on(_._2._1))

    //Print results
    println("Task 1: LineCount")
    println("- businesses    = " + businessCount)
    println("- top_reviewers = " + reviewCount)
    println("- top_users     = " + userCount)
    println("=====================================================================")
    println("Task 2: Top Reviews")
    println("a) Number of distinct users           = " + distinctUserCount)
    println("b) Average review length              = " + avgReviewLength)
    println("c) Top 10 businesses sorted by reviews: " )
    top10Business.map(v => v._1).foreach(v => println("   - " + v))
    println("d) Number of reviews per year: ")
    reviewsPerYear.sortBy(_._1).foreach(y => printf("   %d: %d\n", y._1, y._2))
    printf("e) First and last review:\n   first = %s\n   last = %s\n",
      reviewsMinTimestamp.toString("yyyy/MM/dd hh:mm:ss"),
      reviewsMaxTimestamp.toString("yyyy/MM/dd hh:mm:ss"))
    printf("f) Pearson correlation coefficient = %.6f\n", pcc)
    println("====================================================================")
    println("Task 3: Businesses")
    println("a) Average rating by city:")
    avgBusinessRatingByCity.take(5).foreach(city => printf("   - %s => %.3f\n", city._1, city._2))
    println("b) Top 10 categories:")
    top10BusinessCategories.foreach(category => printf("   - %s\n", category))
    println("c) Postal code centroids: ")
    businessPostalCodeCentroid.take(5).foreach(postalCode => printf("   - %s => %.6f (lat), %.6f (lon)\n",
      postalCode._1,
      postalCode._2._1,
      postalCode._2._2
    ))
    println("Task 4: Friendship graph")
    println("a) Top 10 nodes:")
    println("   IN")
    top10InFriendships.foreach(u => printf("   - %s => %d (in), %d (out)\n", u._1, u._2._1, u._2._2))
    println("   OUT")
    top10OutFriendships.foreach(u => printf("   - %s => %d (in), %d (out)\n", u._1, u._2._1, u._2._2))
    sc.stop()
  }
}