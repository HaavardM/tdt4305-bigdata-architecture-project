import java.nio.charset.StandardCharsets
import java.util.Base64

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source

object part2 {

  val ReviewsIDIndex = 0
  val ReviewsUserIndex = 1
  val ReviewsBusinessIDIndex = 2
  val ReviewsTextIndex = 3
  val ReviewsDateIndex = 4

  def main(args: Array[String]) = {
    val conf = new SparkConf().
      setMaster("local[4]").
      setAppName("part2")
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

    val yelpReviewRDD = sc.textFile("data/yelp_top_reviewers_with_reviews.csv")
      .mapPartitionsWithIndex((index, it) => if (index == 0) it.drop(1) else it)

    //BusinessID => Review Text
    val reviewsRDD = yelpReviewRDD.map(line => {
      val split = line.split('\t')
      val text = new String(Base64.getMimeDecoder.decode(split(ReviewsTextIndex)), StandardCharsets.UTF_8)
      val businessID = split(ReviewsBusinessIDIndex)
      (businessID, text)
    })

    val withoutStopWords = reviewsRDD.map(r => {
      val businessID = r._1
      val text = r._2
      val words = text.split(' ')
      var outputs = ListBuffer[String]()
      for (word <- words) {
        //clean word
        val w = word
          .trim()
          .stripSuffix(",")
          .stripSuffix(".")
          .stripSuffix("!")
          .stripSuffix(":")
          .stripSuffix(";")
          .toLowerCase()
        //add it to the output if it is not a stop word and not empty
        if (!stopWordsBroadcast.value.contains(w) && w != "") {
          outputs += w
        }
      }
      (businessID, outputs.toList)
    })

    val reviewSentimentRDD = withoutStopWords.map(r => {
      val businessIDIndex = r._1
      val sentiments = r._2.map(w => sentimentBroadcast.value.getOrElse(w, 0))
      val sentiment = sentiments.reduceOption(_+_).getOrElse(0)
      (businessIDIndex, sentiment)
    })

    val businessSentimentRDD = reviewSentimentRDD.reduceByKey(_+_)

    //Implicitely define the desired ordering
    implicit val ordering = Ordering.Int.reverse
    val topKreviewsRDD = businessSentimentRDD.sortBy(_._2)
    topKreviewsRDD.coalesce(1).saveAsTextFile("results/topksentiment.csv")
    sc.stop()

  }
}
