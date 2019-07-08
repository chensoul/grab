package com.javachen.grab.repository;

import com.javachen.grab.model.domain.StreamRecs;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.repository.MongoRepository;

/**
 * @author june
 * @createTime 2019-07-07 22:51
 * @see
 * @since
 */
public interface StreamRecsRepository extends MongoRepository<StreamRecs, ObjectId> {
   StreamRecs findByUid(Long uid);
}
