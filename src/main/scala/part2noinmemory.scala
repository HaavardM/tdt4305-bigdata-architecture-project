import java.nio.charset.StandardCharsets
import java.util.Base64

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source

object part2noinmemory {

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

    //Load stop words
    val stopwordsRDD = sc.textFile("stopwords.txt").map((_, false))
    //Load sentiment words
    val sentimentRDD = sc.textFile("AFINN-111.txt").map(l => {
      val split = l.split('\t')
      (split(0), split(1).toInt)
    })
    //Load reviews
    val yelpReviewRDD = sc.textFile("data/yelp_top_reviewers_with_reviews.csv")
      .mapPartitionsWithIndex((index, it) => if (index == 0) it.drop(1) else it)

    //BusinessID => Review Text
    val reviewsRDD = yelpReviewRDD.map(line => {
      val split = line.split('\t')
      val text = new String(Base64.getMimeDecoder.decode(split(ReviewsTextIndex)), StandardCharsets.UTF_8)
      val businessID = split(ReviewsBusinessIDIndex)
      (businessID, text)
    })

    val withoutStopWords = reviewsRDD.flatMap(r => {
      val businessID = r._1
      val text = r._2
      val words = text.split(' ')
      var outputs = ListBuffer[String]()
      for (word <- words) {
        //clean word
        val w = word
          .trim()
          .replaceAll("\\W", "")
          .toLowerCase()
        outputs += w
      }
      outputs.toList.map(w => (w, businessID))
    }).leftOuterJoin(stopwordsRDD)
      .filter(_._2._2.getOrElse(true))
      .map(l => (l._1, l._2._1))

    val businessSentimentRDD = withoutStopWords
      .join(sentimentRDD)
      .map(w => (w._2._1, w._2._2))
      .reduceByKey(_+_)

    //Implicitely define the desired ordering
    implicit val ordering = Ordering.Int.reverse
    val topKreviewsRDD = businessSentimentRDD.sortBy(_._2)
    topKreviewsRDD.coalesce(1).saveAsTextFile("results/topksentiment.csv")
    sc.stop()

  }
}
