package com.javachen.grab.repository;

import com.javachen.grab.model.domain.Movie;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.repository.MongoRepository;

import java.util.List;

/**
 * @author june
 * @createTime 2019-07-07 22:51
 * @see
 * @since
 */
public interface MovieRepository extends MongoRepository<Movie, ObjectId> {
   List<Movie> findFirst10ByOrderByIssueDesc();

   List<Movie> findAllByMidIn(List<Long> mids);

   Movie findByMid(Long mid);

}
