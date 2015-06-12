package com.javachen.grab.mahout.mia.ch02;

import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.CachingRecommender;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

import java.io.File;
import java.util.List;

class RecommenderIntro {

    private RecommenderIntro() {
    }

    public static void main(String[] args) throws Exception {
        File modelFile = null;
        if (args.length > 0)
            modelFile = new File(args[0]);
        if (modelFile == null || !modelFile.exists())
            modelFile = new File("/Users/june/workspace/hadoopProjects/grab/grab-batch/src/main/scala/com/javachen/grab/mahout/mia/ch02/intro.csv");
        if (!modelFile.exists()) {
            System.err.println("Please, specify name of file, or put file 'input.csv' into current directory!");
            System.exit(1);
        }
        DataModel model = new FileDataModel(modelFile);

        //用户相似度，使用基于皮尔逊相关系数计算相似度
        UserSimilarity similarity = new PearsonCorrelationSimilarity(model);

        //选择邻居用户，使用NearestNUserNeighborhood实现UserNeighborhood接口，选择邻近的4个用户
        UserNeighborhood neighborhood =
                new NearestNUserNeighborhood(4, similarity, model);

        Recommender recommender = new GenericUserBasedRecommender(
                model, neighborhood, similarity);

        //
        Recommender cachingRecommender = new CachingRecommender(recommender);

        //给用户1推荐4个物品
        List<RecommendedItem> recommendations =
                recommender.recommend(1, 4);

        for (RecommendedItem recommendation : recommendations) {
            System.out.println(recommendation);
        }

    }

}
