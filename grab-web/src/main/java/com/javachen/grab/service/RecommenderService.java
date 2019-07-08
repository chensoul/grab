package com.javachen.grab.service;

import com.javachen.grab.model.Recommendation;
import com.javachen.grab.model.domain.*;
import com.javachen.grab.repository.*;
import com.javachen.grab.utils.Constant;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.FuzzyQueryBuilder;
import org.elasticsearch.index.query.MoreLikeThisQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class RecommenderService {

    // 混合推荐中CF的比例
    private static Double CF_RATING_FACTOR = 0.3;
    private static Double CB_RATING_FACTOR = 0.3;
    private static Double SR_RATING_FACTOR = 0.4;

    @Autowired
    private MovieRecsRepository movieRecsRepository;

    @Autowired
    private UserRecsRepository userRecsRepository;

    @Autowired
    private StreamRecsRepository streamRecsRepository;

    @Autowired
    private RateMoreRecentlyMoviesRepository rateMoreRecentlyMoviesRepository;

    @Autowired
    private GenresTopMoviesRepository genresTopMoviesRepository;


    @Autowired
    private TransportClient esClient;

    // 协同过滤推荐【电影相似性】
    public List<Recommendation> findMovieCFRecs(Long mid, int maxItems) {
        MovieRecs movieRecs= movieRecsRepository.findByMid(mid);

        if(movieRecs==null){
            return new ArrayList<>();
        }
        List<Recommendation> recommendations = movieRecs.getRecs();
        return recommendations.subList(0, maxItems > recommendations.size() ? recommendations.size() : maxItems);
    }

    // 协同过滤推荐【用户电影矩阵】
    public List<Recommendation> findUserCFRecs(Long uid, int maxItems) {
        UserRecs userRecs= userRecsRepository.findByUid(uid);
        if(userRecs==null){
            return new ArrayList<>();
        }

        List<Recommendation> recommendations = userRecs.getRecs();
        return recommendations.subList(0, maxItems > recommendations.size() ? recommendations.size() : maxItems);
    }

    // 基于内容的推荐算法
    public List<Recommendation> findContentBasedMoreLikeThisRecommendations(Long mid, int maxItems) {
        MoreLikeThisQueryBuilder query = QueryBuilders.moreLikeThisQuery(/*new String[]{"name", "descri", "genres", "actors", "directors", "tags"},*/
                new MoreLikeThisQueryBuilder.Item[]{new MoreLikeThisQueryBuilder.Item(Constant.ES_INDEX, Constant.ES_MOVIE_TYPE, String.valueOf(mid))});

        return parseESResponse(esClient.prepareSearch().setQuery(query).setSize(maxItems).execute().actionGet());
    }

    // 实时推荐
    public List<Recommendation> findStreamRecs(Long uid,int maxItems){
        StreamRecs streamRecs= streamRecsRepository.findByUid(uid);
        if(streamRecs==null){
            return new ArrayList<>();
        }
        List<Recommendation> recommendations = streamRecs.getRecs();
        return recommendations.subList(0, maxItems > recommendations.size() ? recommendations.size() : maxItems);
    }

    // 全文检索
    public List<Recommendation> findContentBasedSearchRecommendations(String text, int maxItems) {
        MultiMatchQueryBuilder query = QueryBuilders.multiMatchQuery(text, "name", "descri");
        return parseESResponse(esClient.prepareSearch().setIndices(Constant.ES_INDEX).setTypes(Constant.ES_MOVIE_TYPE).setQuery(query).setSize(maxItems).execute().actionGet());
    }

    private List<Recommendation> parseESResponse(SearchResponse response) {
        List<Recommendation> recommendations = new ArrayList<>();
        for (SearchHit hit : response.getHits()) {
            recommendations.add(new Recommendation((Long) hit.getSourceAsMap().get("mid"), (double) hit.getScore()));
        }
        return recommendations;
    }

    // 混合推荐算法
    public List<Recommendation> findHybridRecommendations(Long mid, int maxItems) {
        List<Recommendation> hybridRecommendations = new ArrayList<>();

        List<Recommendation> cfRecs = findMovieCFRecs(mid, maxItems);
        for (Recommendation recommendation : cfRecs) {
            hybridRecommendations.add(new Recommendation(recommendation.getMid(), recommendation.getScore() * CF_RATING_FACTOR));
        }

        List<Recommendation> cbRecs = findContentBasedMoreLikeThisRecommendations(mid, maxItems);
        for (Recommendation recommendation : cbRecs) {
            hybridRecommendations.add(new Recommendation(recommendation.getMid(), recommendation.getScore() * CB_RATING_FACTOR));
        }

        List<Recommendation> streamRecs = findStreamRecs(mid,maxItems);
        for (Recommendation recommendation : streamRecs) {
            hybridRecommendations.add(new Recommendation(recommendation.getMid(), recommendation.getScore() * SR_RATING_FACTOR));
        }

        return hybridRecommendations.subList(0, maxItems > hybridRecommendations.size() ? hybridRecommendations.size() : maxItems);
    }


    public List<Recommendation> getHotRecommendations() {
        List<RateMoreRecentlyMovies> rateMoreRecentlyMovies=rateMoreRecentlyMoviesRepository.findTop10ByOrderByYearmonthDesc();

        List<Recommendation> recommendations = new ArrayList<>();
        for (RateMoreRecentlyMovies recentlyMovies : rateMoreRecentlyMovies) {
            recommendations.add(new Recommendation(recentlyMovies.getMid(), 0D));
        }
        return recommendations;
    }

    public List<Recommendation> getRateMoreRecommendations() {
        // 获取评分最多电影的条目
        List<RateMoreRecentlyMovies> rateMoreRecentlyMovies=rateMoreRecentlyMoviesRepository.findTop10ByOrderByNumDesc();

        List<Recommendation> recommendations = new ArrayList<>();
        for (RateMoreRecentlyMovies recentlyMovies : rateMoreRecentlyMovies) {
            recommendations.add(new Recommendation(recentlyMovies.getMid(), 0D));
        }
        return recommendations;
    }

    public List<Recommendation> getContentBasedGenresRecommendations(String text,Integer maxItems) {
        FuzzyQueryBuilder query = QueryBuilders.fuzzyQuery("genres", maxItems);
        return parseESResponse(esClient.prepareSearch().setIndices(Constant.ES_INDEX).setTypes(Constant.ES_MOVIE_TYPE).setQuery(query).setSize(maxItems).execute().actionGet());
    }

    public List<Recommendation> getTopGenresRecommendations(String genres,Integer maxItems){
        GenresTopMovies genresTopMovies=genresTopMoviesRepository.findAllByGenres(genres);

        List<Recommendation> recommendations = genresTopMovies.getRecs();
        return recommendations.subList(0, maxItems > recommendations.size() ? recommendations.size() : maxItems);
    }

}