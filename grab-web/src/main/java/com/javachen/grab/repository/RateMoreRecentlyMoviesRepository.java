package com.javachen.grab.repository;

import com.javachen.grab.model.domain.RateMoreRecentlyMovies;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.repository.MongoRepository;

import java.util.List;

/**
 * @author june
 * @createTime 2019-07-08 13:29
 * @see
 * @since
 */
public interface RateMoreRecentlyMoviesRepository extends MongoRepository<RateMoreRecentlyMovies, ObjectId> {
    List<RateMoreRecentlyMovies> findTop10ByOrderByYearmonthDesc();

    List<RateMoreRecentlyMovies> findTop10ByOrderByNumDesc();
}
