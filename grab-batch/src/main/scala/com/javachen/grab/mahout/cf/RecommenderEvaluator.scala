package com.javachen.grab.mahout.cf

import java.io.{File, PrintWriter}

import org.apache.mahout.cf.taste.common.Weighting
import org.apache.mahout.cf.taste.eval.{IRStatistics, RecommenderBuilder}
import org.apache.mahout.cf.taste.impl.eval.GenericRecommenderIRStatsEvaluator
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel
import org.apache.mahout.cf.taste.impl.model.{GenericBooleanPrefDataModel, GenericDataModel}
import org.apache.mahout.cf.taste.impl.neighborhood.{NearestNUserNeighborhood, ThresholdUserNeighborhood}
import org.apache.mahout.cf.taste.impl.recommender.{GenericItemBasedRecommender, GenericUserBasedRecommender}
import org.apache.mahout.cf.taste.impl.similarity.{CityBlockSimilarity, EuclideanDistanceSimilarity, LogLikelihoodSimilarity, PearsonCorrelationSimilarity, TanimotoCoefficientSimilarity, UncenteredCosineSimilarity}
import org.apache.mahout.cf.taste.model.DataModel
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood
import org.apache.mahout.cf.taste.recommender.Recommender
import org.apache.mahout.cf.taste.similarity.{ItemSimilarity, UserSimilarity}

import scala.collection.mutable.HashMap
import scala.io.Source
import scala.util.Random

/**
 * https://github.com/sujitpal/mia-scala-examples
 */
object RecommenderEvaluator {
  val neighborhoods = Array("nearest", "threshold")
  val similarities = Array("euclidean", "pearson", "pearson_w", "cosine", "cosine_w", "manhattan", "llr", "tanimoto")
  val simThreshold = 0.5
  val sampleFileName = "/tmp/recommender-evaluator-sample.tmp.csv"

  def main(args: Array[String]): Unit = {
    val argmap = parseArgs(args)

    val filename = if (argmap("sample_pct") != null) {
      val nlines = Source.fromFile(argmap("filename")).size
      val sampleSize = nlines * (argmap("sample_pct").toFloat / 100.0)
      val rand = new Random(System.currentTimeMillis())
      var sampleLineNos = Set[Int]()
      do {
        sampleLineNos += rand.nextInt(nlines)
      } while (sampleLineNos.size < sampleSize)
      val out = new PrintWriter(sampleFileName)
      var currline = 0
      for (line <- Source.fromFile(argmap("filename")).getLines) {
        if (sampleLineNos.contains(currline)) {
          out.println(line)
        }
        currline += 1
      }
      out.close()
      sampleFileName
    } else {
      argmap("filename")
    }

    val model = argmap("bool") match {
      case "false" => new GenericDataModel(
        GenericDataModel.toDataMap(new FileDataModel(
          new File(filename))))
      case "true" => new GenericBooleanPrefDataModel(
        GenericBooleanPrefDataModel.toDataMap(new FileDataModel(
          new File(filename))))
      case _ => throw new IllegalArgumentException(
        invalidValue("bool", argmap("bool")))
    }
    val evaluator = new GenericRecommenderIRStatsEvaluator()

    argmap("type") match {
      case "user" => {
        for (neighborhood <- neighborhoods;
             similarity <- similarities) {
          println("Processing " + neighborhood + " / " + similarity)
          try {
            val recommenderBuilder = userRecoBuilder(
              neighborhood, similarity,
              model.asInstanceOf[GenericDataModel])
            val stats = evaluator.evaluate(recommenderBuilder,
              null, model,
              null, argmap("precision_point").toInt,
              GenericRecommenderIRStatsEvaluator.CHOOSE_THRESHOLD,
              argmap("eval_fract").toDouble)
            printResult(neighborhood, similarity, stats)
          } catch {
            case e => {
              println("Exception caught: " + e.getMessage)
            }
          }
        }
      }
      case "item" => {
        for (similarity <- similarities) {
          println("Processing " + similarity)
          try {
            val recommenderBuilder = itemRecoBuilder(similarity,
              model.asInstanceOf[GenericDataModel])
            val stats = evaluator.evaluate(recommenderBuilder, null,
              model, null, argmap("precision_point").toInt,
              GenericRecommenderIRStatsEvaluator.CHOOSE_THRESHOLD,
              argmap("eval_fract").toDouble)
            printResult(null, similarity, stats)
          } catch {
            case e => {
              println("Exception caught: " + e.getMessage)
            }
          }
        }
      }
      case _ => throw new IllegalArgumentException(
        invalidValue("type", argmap("type")))
    }
  }

  def parseArgs(args: Array[String]): HashMap[String, String] = {
    val argmap = new HashMap[String, String]()
    for (arg <- args) {
      val nvp = arg.split("=")
      argmap(nvp(0)) = nvp(1)
    }
    argmap
  }

  def itemRecoBuilder(similarity: String,
                      model: GenericDataModel): RecommenderBuilder = {
    val s: ItemSimilarity = similarity match {
      case "euclidean" => new EuclideanDistanceSimilarity(model)
      case "pearson" => new PearsonCorrelationSimilarity(model)
      case "pearson_w" => new PearsonCorrelationSimilarity(
        model, Weighting.WEIGHTED)
      case "cosine" => new UncenteredCosineSimilarity(model)
      case "cosine_w" => new UncenteredCosineSimilarity(
        model, Weighting.WEIGHTED)
      case "manhattan" => new CityBlockSimilarity(model)
      case "llr" => new LogLikelihoodSimilarity(model)
      case "tanimoto" => new TanimotoCoefficientSimilarity(model)
      case _ => throw new IllegalArgumentException(
        invalidValue("similarity", similarity))
    }
    new RecommenderBuilder() {
      override def buildRecommender(model: DataModel): Recommender = {
        new GenericItemBasedRecommender(model, s)
      }
    }
  }

  def userRecoBuilder(neighborhood: String,
                      similarity: String,
                      model: GenericDataModel): RecommenderBuilder = {
    val s: UserSimilarity = similarity match {
      case "euclidean" => new EuclideanDistanceSimilarity(model)
      case "pearson" => new PearsonCorrelationSimilarity(model)
      case "pearson_w" => new PearsonCorrelationSimilarity(
        model, Weighting.WEIGHTED)
      case "cosine" => new UncenteredCosineSimilarity(model)
      case "cosine_w" => new UncenteredCosineSimilarity(
        model, Weighting.WEIGHTED)
      case "manhattan" => new CityBlockSimilarity(model)
      case "llr" => new LogLikelihoodSimilarity(model)
      case "tanimoto" => new TanimotoCoefficientSimilarity(model)
      case _ => throw new IllegalArgumentException(
        invalidValue("similarity", similarity))
    }
    val neighborhoodSize = if (model.getNumUsers > 10)
      (model.getNumUsers / 10)
    else (model.getNumUsers)
    val n: UserNeighborhood = neighborhood match {
      case "nearest" => new NearestNUserNeighborhood(
        neighborhoodSize, s, model)
      case "threshold" => new ThresholdUserNeighborhood(
        simThreshold, s, model)
      case _ => throw new IllegalArgumentException(
        invalidValue("neighborhood", neighborhood))
    }
    new RecommenderBuilder() {
      override def buildRecommender(model: DataModel): Recommender = {
        new GenericUserBasedRecommender(model, n, s)
      }
    }
  }

  def invalidValue(key: String, value: String): String = {
    "Invalid value for '" + key + "': " + value
  }

  def printResult(neighborhood: String,
                  similarity: String,
                  stats: IRStatistics): Unit = {
    println(">>> " +
      (if (neighborhood != null) neighborhood else "") +
      "\t" + similarity +
      "\t" + stats.getPrecision.toString +
      "\t" + stats.getRecall.toString)
  }

  def usage(): Unit = {
    println("Usage:")
    println("com.mycompany.mia.cf.RecommenderEvaluator [-key=value...]")
    println("where:")
    println("sample_pct=0-100 (use sample_pct of original input)")
    println("type=user|item (the type of recommender to build)")
    println("bool=true|false (whether to use boolean or actual preferences)")
    println("precision_point=n (the precision at n desired)")
    println("eval_fract=n (fraction of data to use for evaluation)")
    System.exit(1)
  }
}