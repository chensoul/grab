package com.javachen.grab.mahout.mia.ch04;

import org.apache.mahout.cf.taste.impl.eval.LoadEvaluator;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;
import org.apache.mahout.cf.taste.similarity.precompute.example.GroupLensDataModel;

import java.io.File;

class GroupLensDataModelIntro {

    private GroupLensDataModelIntro() {
    }

    public static void main(String[] args) throws Exception {
        DataModel model = new GroupLensDataModel(new File("/Users/june/Documents/tmp/spark-1.3.0-bin-cdh4/data/ml-1m/ratings.dat"));
        UserSimilarity similarity = new PearsonCorrelationSimilarity(model);
        UserNeighborhood neighborhood =
                new NearestNUserNeighborhood(100, similarity, model);
        Recommender recommender =
                new GenericUserBasedRecommender(model, neighborhood, similarity);
        LoadEvaluator.runLoad(recommender);
    }

}
