import java.nio.charset.StandardCharsets
import java.util.Base64

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source

object sentiment_analysis {

  val ReviewsIDIndex = 0
  val ReviewsUserIndex = 1
  val ReviewsBusinessIDIndex = 2
  val ReviewsTextIndex = 3
  val ReviewsDateIndex = 4

  def main(args: Array[String]) = {
    val conf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("part2")
      //.set("spark.hadoop.validateOutputSpecs", "false") //Avoid error when resultfile exists already
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    //Load stop words to set
    val rawStopWords = mutable.HashSet[String]()
    for(line <- Source.fromFile("stopwords.txt").getLines) {
      rawStopWords.add(line)
    }
    //Broadcast stop words to all nodes
    val stopWordsBroadcast = sc.broadcast(rawStopWords.toSet)

    //Load word sentiment to hash map
    val rawSentiment = mutable.HashMap[String, Int]()
    for (line <- Source.fromFile("AFINN-111.txt").getLines()) {
      val split = line.split('\t')
      rawSentiment(split(0)) = split(1).toInt
    }
    //Broadcast sentiments to all nodes
    val sentimentBroadcast = sc.broadcast(rawSentiment.toMap)

    //Load reviews into RDD, drop first row (column description)
    val yelpReviewRDD = sc.textFile("data/yelp_top_reviewers_with_reviews.csv")
      .mapPartitionsWithIndex((index, it) => if (index == 0) it.drop(1) else it)

    //Create key value pair from business_id to review text
    //review ID is omitted as it is not of any interest
    val reviewsRDD = yelpReviewRDD.map(line => {
      //Split by tabular
      val split = line.split('\t')
      //Decode base64 review text and get bytes
      val b64 = Base64.getMimeDecoder.decode(split(ReviewsTextIndex))
      //Interpret bytes as UTF_8 and create string
      val text = new String(b64, StandardCharsets.UTF_8)
      //Get business id
      val businessID = split(ReviewsBusinessIDIndex)
      //Return key-value pair
      (businessID, text)
    })

    //Remove stopwords
    val withoutStopWordsRDD = reviewsRDD.map(r => {
      val businessID = r._1
      val text = r._2
      //Clean and filter words in memory
      val words = text
        .split(' ')
        .map(_.trim() //Remove whitespace
              .replaceAll("\\W", "") //Remove all non-word-characters using regex
              .toLowerCase() //Convert to lower case
        ).filter(word => {
          //Only keep words which are not empty and not stopwords
          !word.isEmpty() && !stopWordsBroadcast.value.contains(word)
        })
      (businessID, words.toList)
    })

    //reviewSentimentRDD contains key-value pairs,
    //businessID => sentiment, one for each review.
    val reviewSentimentRDD = withoutStopWordsRDD.map(r => {
      val businessIDIndex = r._1
      val words = r._2
      //Map words to sentiment, or zero if word not found
      val sentiments = words.map(w => sentimentBroadcast.value.getOrElse(w, 0))
      //Aggregate sentiments for all words in a review
      val sentiment = sentiments.reduceOption(_+_).getOrElse(0)
      //Return sentiment for one review for a business
      (businessIDIndex, sentiment)
    })

    //Aggregate sentiments from all reviews for each business
    val businessSentimentRDD = reviewSentimentRDD.reduceByKey(_+_)


    /*
    Sorting all businesses by sentiment
    We could also have used businessSentimentRDD.takeOrdered(k) to get the top-k businesses more effeciently. 
    We did not want to assume k-elements neccessarily was small enough to fit on one computer,
    so we used sort to continue working with RDDs. 
    */

    //Implicitely define the desired ordering to be descending order
    implicit val ordering = Ordering.Int.reverse
    //Sort by sentiment
    val topReviewsRDD = businessSentimentRDD.sortBy(_._2)//.persist()

    
    /*******From now on we assume the result set is small enough to fit in memory on one computer******/
    //Take k top
    //uncomment persist above to speed up the computation
    val k = 10
    //Already ordered, so we take the first k elements using take
    //and print them to the console
    topReviewsRDD.take(k).zipWithIndex foreach { case(el, i) =>
      printf("%d: Business %s => sentiment %d\n", i, el._1, el._2)
    }
    
    //Combine all businesses into one partition and save to textfile
    topReviewsRDD.coalesce(1).saveAsTextFile("results/topksentiment.csv")

    sc.stop()

  }
}
