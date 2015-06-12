package com.javachen.grab.mahout.mia.cf;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.*;
import org.apache.mahout.cf.taste.impl.common.FastByIDMap;
import org.apache.mahout.cf.taste.impl.eval.AverageAbsoluteDifferenceRecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.eval.GenericRecommenderIRStatsEvaluator;
import org.apache.mahout.cf.taste.impl.model.GenericBooleanPrefDataModel;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.recommender.GenericItemBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.LogLikelihoodSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.model.PreferenceArray;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;

import java.io.File;
import java.util.List;

/**
 * 基于物品的推荐
 * <p/>
 * Created by <a href="mailto:junechen@163.com">june</a> on 2015-06-10 10:07.
 */
public class ItemBasedBooleanRecommender {
    public static void main(String[] args) throws Exception {
        //创建模型，无偏好值
        String file = "/Users/june/workspace/hadoopProjects/grab/grab-batch/src/main/scala/com/javachen/grab/mahout/mia/ch02/intro.csv";

        DataModel model = new GenericBooleanPrefDataModel(
                GenericBooleanPrefDataModel.toDataMap(new FileDataModel(new File(file))));

        // Build the same recommender for testing that we did last time:
        RecommenderBuilder recommenderBuilder = new RecommenderBuilder() {
            @Override
            public Recommender buildRecommender(DataModel model) throws TasteException {
                ItemSimilarity similarity = new LogLikelihoodSimilarity(model);
                return new GenericItemBasedRecommender(model, similarity);
            }
        };

        //获取推荐结果
        List<RecommendedItem> recommendations = recommenderBuilder.buildRecommender(model).recommend(1, 10);

        for (RecommendedItem recommendation : recommendations) {
            System.out.println(recommendation);
        }


        //计算评分
        RecommenderEvaluator evaluator =
                new AverageAbsoluteDifferenceRecommenderEvaluator();
        DataModelBuilder modelBuilder = new DataModelBuilder() {
            @Override
            public DataModel buildDataModel(FastByIDMap<PreferenceArray> trainingData) {
                return new GenericBooleanPrefDataModel(
                        GenericBooleanPrefDataModel.toDataMap(trainingData));
            }
        };
        double score = evaluator.evaluate(
                recommenderBuilder, modelBuilder, model, 0.9, 1.0);
        System.out.println(score);

        //计算查全率和查准率
        RecommenderIRStatsEvaluator statsEvaluator = new GenericRecommenderIRStatsEvaluator();

        // Evaluate precision and recall "at 2":
        IRStatistics stats = statsEvaluator.evaluate(recommenderBuilder,
                null, model, null, 2,
                GenericRecommenderIRStatsEvaluator.CHOOSE_THRESHOLD,
                1.0);
        System.out.println(stats.getPrecision());
        System.out.println(stats.getRecall());
    }
}
