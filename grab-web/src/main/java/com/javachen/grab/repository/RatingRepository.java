package com.javachen.grab.repository;

import com.javachen.grab.model.domain.Rating;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.repository.MongoRepository;

import java.util.List;

/**
 * @author june
 * @createTime 2019-07-07 22:51
 * @see
 * @since
 */
public interface RatingRepository extends MongoRepository<Rating, ObjectId> {
    List<Rating> findAllByUid(Long uid);

    Rating findByUidAndMid(Long uid,Long mid);
}
