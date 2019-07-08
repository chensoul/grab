package com.javachen.grab.rest;

import com.javachen.grab.model.domain.Movie;
import com.javachen.grab.model.domain.Rating;
import com.javachen.grab.model.domain.Tag;
import com.javachen.grab.model.domain.User;
import com.javachen.grab.model.recom.Recommendation;
import com.javachen.grab.service.*;
import com.javachen.grab.utils.Constant;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.util.*;


@RequestMapping("/rest/movie")
@Controller
@Slf4j
public class MovieRestApi {

    @Autowired
    private RecommenderService recommenderService;
    @Autowired
    private MovieService movieService;
    @Autowired
    private UserService userService;
    @Autowired
    private RatingService ratingService;
    @Autowired
    private TagService tagService;

    /**
     * 获取推荐的电影【实时推荐6 + 内容推荐4】
     * @param username
     * @return
     */
    // TODO: 2017/10/20  bug 混合推荐结果中，基于内容的推荐，基于MID，而非UID
    @RequestMapping(value = "/guess", method = RequestMethod.GET )
    @ResponseBody
    public List<Movie> getGuessMovies(@RequestParam("username")String username,@RequestParam("num")int num) {
        User user = userService.findByUsername(username);
        List<Recommendation> recommendations = recommenderService.findHybridRecommendations(user.getUid(),num);
        if(recommendations.size()==0){
            String randomGenres = user.getPrefGenres().get(new Random().nextInt(user.getPrefGenres().size()));
            recommendations = recommenderService.getTopGenresRecommendations(randomGenres.split(" ")[0],num);
        }
        return movieService.getHybirdRecommendeMovies(recommendations);
    }

    /**
     *
     * @param username
     * @param num
     * @return
     */
    @RequestMapping(value = "/wish",method = RequestMethod.GET )
    @ResponseBody
    public List<Movie> getWishMovies(@RequestParam("username")String username,@RequestParam("num")int num) {
        User user = userService.findByUsername(username);
        List<Recommendation> recommendations = recommenderService.findUserCFRecs(user.getUid(),num);
        if(recommendations.size()==0){
            String randomGenres = user.getPrefGenres().get(new Random().nextInt(user.getPrefGenres().size()));
            recommendations = recommenderService.getTopGenresRecommendations(randomGenres.split(" ")[0],num);
        }
        return movieService.getRecommendeMovies(recommendations);
    }

    /**
     * 获取热门推荐
     * @return
     */
    @RequestMapping(value = "/hot",  method = RequestMethod.GET )
    @ResponseBody
    public List<Movie> getHotMovies(@RequestParam("num")int num) {
        List<Recommendation> recommendations = recommenderService.getHotRecommendations();
        return movieService.getRecommendeMovies(recommendations);
    }

    /**
     * 获取投票最多的电影
     * @return
     */
    @RequestMapping(value = "/rate",  method = RequestMethod.GET )
    @ResponseBody
    public List<Movie> getRateMoreMovies(@RequestParam("num")int num) {
        List<Recommendation> recommendations = recommenderService.getRateMoreRecommendations();
        return movieService.getRecommendeMovies(recommendations);
    }

    /**
     * 获取新添加的电影
     * @return
     */
    @RequestMapping(value = "/new",  method = RequestMethod.GET )
    @ResponseBody
    public List<Movie> getNewMovies() {
        return movieService.getNewMovies();
    }

    /**
     * 获取电影详细页面相似的电影集合
     * @return
     */
    @RequestMapping(value = "/same/{mid}",method = RequestMethod.GET )
    @ResponseBody
    public List<Movie> getSameMovie(@PathVariable("mid")Long mid,@RequestParam("num")int num) {
        List<Recommendation> recommendations = recommenderService.findMovieCFRecs(mid,num);
        return movieService.getRecommendeMovies(recommendations);
    }


    /**
     * 获取单个电影的信息
     * @param mid
     * @return
     */
    @RequestMapping(value = "/info/{mid}", method = RequestMethod.GET )
    @ResponseBody
    public Movie getMovieInfo(@PathVariable("mid")Long mid) {
        return movieService.findByMid(mid);
    }

    /**
     * 模糊查询电影
     * @param query
     * @return
     */
    @RequestMapping(value = "/search",method = RequestMethod.GET )
    @ResponseBody
    public List<Movie> getSearchMovies(@RequestParam("query")String query) {
//        List<Recommendation> recommendations = recommenderService.getContentBasedSearchRecommendations(new SearchRecommendationRequest(query,100));
//        model.addAttribute("success",true);
//        model.addAttribute("movies",movieService.getRecommendeMovies(recommendations));
        return null;
    }

//    /**
//     * 查询类别电影
//     * @param category
//     * @param model
//     * @return
//     */
//    @RequestMapping(value = "/genres", produces = "application/json", method = RequestMethod.GET )
//    @ResponseBody
//    public Model getGenresMovies(@RequestParam("category")String category, Model model) {
//        List<Recommendation> recommendations = recommenderService.getContentBasedGenresRecommendations(new SearchRecommendationRequest(category,100));
//        model.addAttribute("success",true);
//        model.addAttribute("movies",movieService.getRecommendeMovies(recommendations));
//        return model;
//    }

    /**
     * 获取用户评分过得电影
     * @param username
     * @return
     */
    @RequestMapping(value = "/myrate", method = RequestMethod.GET )
    @ResponseBody
    public List<Movie>  getMyRateMovies(@RequestParam("username")String username) {
        User user = userService.findByUsername(username);
        List<Rating> ratings=ratingService.findAllByUid(user.getUid());

        List<Long> ids = new ArrayList<>();
        Map<Long,Double> scores = new HashMap<>();
        for (Rating rating: ratings) {
            ids.add(rating.getMid());
            scores.put(rating.getMid(),rating.getScore());
        }
        List<Movie> movies = movieService.findAllByMidIn(ids);
        for (Movie movie: movies) {
            movie.setScore(scores.getOrDefault(movie.getMid(),movie.getScore()));
        }

        return movies;
    }


    @RequestMapping(value = "/rate/{mid}",  method = RequestMethod.GET )
    @ResponseBody
    public Rating rateToMovie(@PathVariable("mid")Long mid,@RequestParam("score")Double score,@RequestParam("username")String username) {
        User user = userService.findByUsername(username);
        Rating rating = new Rating(user.getUid(),mid,score);
        ratingService.newRating(rating);
        //埋点日志
        System.out.print("=========complete=========");
        log.info(Constant.MOVIE_RATING_PREFIX + ":" + user.getUid() +"|"+ mid +"|"+ score +"|"+ System.currentTimeMillis()/1000);
        return rating;
    }


    @RequestMapping(value = "/tag/{mid}",method = RequestMethod.GET )
    @ResponseBody
    public List<Tag> getMovieTags(@PathVariable("mid")Long mid) {
        return tagService.findAllByMid(mid);
    }

    @RequestMapping(value = "/mytag/{mid}", method = RequestMethod.GET )
    @ResponseBody
    public List<Tag> getMyTags(@PathVariable("mid")Long mid, @RequestParam("username")String username) {
        User user = userService.findByUsername(username);
        return tagService.findAllByUidAndMid(user.getUid(),mid);
    }

    @RequestMapping(value = "/newtag/{mid}", method = RequestMethod.GET )
    @ResponseBody
    public Tag addMyTags(@PathVariable("mid")Long mid,@RequestParam("tagname")String tagname,@RequestParam("username")String username) {
        User user = userService.findByUsername(username);
        Tag tag = new Tag(user.getUid(),mid,tagname);
        tagService.newTag(tag);
        return tag;
    }

    @RequestMapping(value = "/stat", method = RequestMethod.GET )
    @ResponseBody
    public int[] getMyRatingStat(@RequestParam("username")String username) {
        User user = userService.findByUsername(username);
        if(user!=null){
            return ratingService.getMyRatingStat(user.getUid());
        }
        return null;
    }

}
