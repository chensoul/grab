package com.javachen.grab.service;

import com.javachen.grab.model.domain.Movie;
import com.javachen.grab.model.recom.Recommendation;
import com.javachen.grab.repository.MovieRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class MovieService {

    @Autowired
    private MovieRepository movieRepository;

    private List<Movie> findAll(){
       return movieRepository.findAll();
    }

    public List<Movie> getRecommendeMovies(List<Recommendation> recommendations){
        List<Long> ids = new ArrayList<>();
        for (Recommendation rec: recommendations) {
            ids.add(rec.getMid());
        }
        return findAllByMidIn(ids);
    }

    public List<Movie> getHybirdRecommendeMovies(List<Recommendation> recommendations){
        List<Long> ids = new ArrayList<>();
        for (Recommendation rec: recommendations) {
            ids.add(rec.getMid());
        }
        return findAllByMidIn(ids);
    }

    public List<Movie> findAllByMidIn(List<Long> mids){
        return movieRepository.findAllByMidIn(mids);
    }

    public boolean movieExist(Long mid){
        return null != findByMid(mid);
    }

    public Movie findByMid(Long mid){
        return movieRepository.findByMid(mid);
    }

    public void removeMovie(Long mid){
        Movie movie=findByMid(mid);
        movieRepository.delete(movie);
    }

    public List<Movie> getNewMovies(){
        return movieRepository.findFirst10ByOrderByIssueDesc();
    }
}
