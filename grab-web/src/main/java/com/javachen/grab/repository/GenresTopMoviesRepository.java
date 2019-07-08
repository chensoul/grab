package com.javachen.grab.repository;

import com.javachen.grab.model.domain.GenresTopMovies;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.repository.MongoRepository;

/**
 * @author june
 * @createTime 2019-07-08 13:38
 * @see
 * @since
 */
public interface GenresTopMoviesRepository extends MongoRepository<GenresTopMovies, ObjectId> {
    GenresTopMovies findAllByGenres(String genres);
}
