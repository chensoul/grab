package com.javachen.grab.examples.mllib

import org.apache.spark
import org.apache.spark.SparkContext
import spark.SparkContext
import SparkContext._

/**
 * https://gist.github.com/MLnick/5286475
 * A port of [[http://blog.echen.me/2012/02/09/movie-recommendations-and-more-via-mapreduce-and-scalding/]]
 * to Spark.
 * Uses movie ratings data from MovieLens 100k dataset found at [[http://www.grouplens.org/node/73]]
 */
object MovieSimilarities {

  def main(args: Array[String]) {
    /**
     * Parameters to regularize correlation.
     */
    val PRIOR_COUNT = 10
    val PRIOR_CORRELATION = 0

    val TRAIN_FILENAME = "data/ml-100k/ua.base"
    val TEST_FIELNAME = "data/ml-100k/ua.test"
    val MOVIES_FILENAME = "data/ml-100k/u.item"

    /**
     * Spark programs require a SparkContext to be initialized
     */
    val master = "local[*]"
    val sc = new SparkContext(master, "MovieSimilarities")

    // get movie names keyed on id
    val movies = sc.textFile(MOVIES_FILENAME)
      .map(line => {
      val fields = line.split("\\|")
      (fields(0).toInt, fields(1))
    })
    val movieNames = movies.collectAsMap()    // for local use to map id <-> movie name for pretty-printing

    // extract (userid, movieid, rating) from ratings data
    val ratings = sc.textFile(TRAIN_FILENAME)
      .map(line => {
      val fields = line.split("\t")
      (fields(0).toInt, fields(1).toInt, fields(2).toInt)
    })

    // get num raters per movie, keyed on movie id
    val numRatersPerMovie = ratings
      .groupBy(tup => tup._2)
      .map(grouped => (grouped._1, grouped._2.size))

    // join ratings with num raters on movie id
    val ratingsWithSize = ratings
      .groupBy(tup => tup._2)
      .join(numRatersPerMovie)
      .flatMap(joined => {
      joined._2._1.map(f => (f._1, f._2, f._3, joined._2._2))
    })

    // ratingsWithSize now contains the following fields: (user, movie, rating, numRaters).

    // dummy copy of ratings for self join
    val ratings2 = ratingsWithSize.keyBy(tup => tup._1)

    // join on userid and filter movie pairs such that we don't double-count and exclude self-pairs
    val ratingPairs =
      ratingsWithSize
        .keyBy(tup => tup._1)
        .join(ratings2)
        .filter(f => f._2._1._2 < f._2._2._2)

    // compute raw inputs to similarity metrics for each movie pair
    val vectorCalcs =
      ratingPairs
        .map(data => {
        val key = (data._2._1._2, data._2._2._2)
        val stats =
          (data._2._1._3 * data._2._2._3, // rating 1 * rating 2
            data._2._1._3,                // rating movie 1
            data._2._2._3,                // rating movie 2
            math.pow(data._2._1._3, 2),   // square of rating movie 1
            math.pow(data._2._2._3, 2),   // square of rating movie 2
            data._2._1._4,                // number of raters movie 1
            data._2._2._4)                // number of raters movie 2
        (key, stats)
      })
        .groupByKey()
        .map(data => {
        val key = data._1
        val vals = data._2
        val size = vals.size
        val dotProduct = vals.map(f => f._1).sum
        val ratingSum = vals.map(f => f._2).sum
        val rating2Sum = vals.map(f => f._3).sum
        val ratingSq = vals.map(f => f._4).sum
        val rating2Sq = vals.map(f => f._5).sum
        val numRaters = vals.map(f => f._6).max
        val numRaters2 = vals.map(f => f._7).max
        (key, (size, dotProduct, ratingSum, rating2Sum, ratingSq, rating2Sq, numRaters, numRaters2))
      })

    // compute similarity metrics for each movie pair
    val similarities =
      vectorCalcs
        .map(fields => {
        val key = fields._1
        val (size, dotProduct, ratingSum, rating2Sum, ratingNormSq, rating2NormSq, numRaters, numRaters2) = fields._2
        val corr = correlation(size, dotProduct, ratingSum, rating2Sum, ratingNormSq, rating2NormSq)
        val regCorr = regularizedCorrelation(size, dotProduct, ratingSum, rating2Sum,
          ratingNormSq, rating2NormSq, PRIOR_COUNT, PRIOR_CORRELATION)
        val cosSim = cosineSimilarity(dotProduct, scala.math.sqrt(ratingNormSq), scala.math.sqrt(rating2NormSq))
        val jaccard = jaccardSimilarity(size, numRaters, numRaters2)

        (key, (corr, regCorr, cosSim, jaccard))
      })

    // test a few movies out (substitute the contains call with the relevant movie name
    val sample = similarities.filter(m => {
      val movies = m._1
      (movieNames(movies._1).contains("Star Wars (1977)"))
    })

    // collect results, excluding NaNs if applicable
    val result = sample.map(v => {
      val m1 = v._1._1
      val m2 = v._1._2
      val corr = v._2._1
      val rcorr = v._2._2
      val cos = v._2._3
      val j = v._2._4
      (movieNames(m1), movieNames(m2), corr, rcorr, cos, j)
    }).collect().filter(e => !(e._4 equals Double.NaN))    // test for NaNs must use equals rather than ==
      .sortBy(elem => elem._4).take(10)

    // print the top 10 out
    result.foreach(r => println(r._1 + " | " + r._2 + " | " + r._3.formatted("%2.4f") + " | " + r._4.formatted("%2.4f")
      + " | " + r._5.formatted("%2.4f") + " | " + r._6.formatted("%2.4f")))
  }

  // *************************
  // * SIMILARITY MEASURES
  // *************************

  /**
   * The correlation between two vectors A, B is
   *   cov(A, B) / (stdDev(A) * stdDev(B))
   *
   * This is equivalent to
   *   [n * dotProduct(A, B) - sum(A) * sum(B)] /
   *     sqrt{ [n * norm(A)^2 - sum(A)^2] [n * norm(B)^2 - sum(B)^2] }
   */
  def correlation(size : Double, dotProduct : Double, ratingSum : Double,
                  rating2Sum : Double, ratingNormSq : Double, rating2NormSq : Double) = {

    val numerator = size * dotProduct - ratingSum * rating2Sum
    val denominator = scala.math.sqrt(size * ratingNormSq - ratingSum * ratingSum) *
      scala.math.sqrt(size * rating2NormSq - rating2Sum * rating2Sum)

    numerator / denominator
  }

  /**
   * Regularize correlation by adding virtual pseudocounts over a prior:
   *   RegularizedCorrelation = w * ActualCorrelation + (1 - w) * PriorCorrelation
   * where w = # actualPairs / (# actualPairs + # virtualPairs).
   */
  def regularizedCorrelation(size : Double, dotProduct : Double, ratingSum : Double,
                             rating2Sum : Double, ratingNormSq : Double, rating2NormSq : Double,
                             virtualCount : Double, priorCorrelation : Double) = {

    val unregularizedCorrelation = correlation(size, dotProduct, ratingSum, rating2Sum, ratingNormSq, rating2NormSq)
    val w = size / (size + virtualCount)

    w * unregularizedCorrelation + (1 - w) * priorCorrelation
  }

  /**
   * The cosine similarity between two vectors A, B is
   *   dotProduct(A, B) / (norm(A) * norm(B))
   */
  def cosineSimilarity(dotProduct : Double, ratingNorm : Double, rating2Norm : Double) = {
    dotProduct / (ratingNorm * rating2Norm)
  }

  /**
   * The Jaccard Similarity between two sets A, B is
   *   |Intersection(A, B)| / |Union(A, B)|
   */
  def jaccardSimilarity(usersInCommon : Double, totalUsers1 : Double, totalUsers2 : Double) = {
    val union = totalUsers1 + totalUsers2 - usersInCommon
    usersInCommon / union
  }

}