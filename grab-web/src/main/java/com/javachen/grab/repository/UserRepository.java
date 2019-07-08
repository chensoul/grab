package com.javachen.grab.repository;

import com.javachen.grab.model.domain.User;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.repository.MongoRepository;

/**
 * @author june
 * @createTime 2019-07-07 22:51
 * @see
 * @since
 */
public interface UserRepository extends MongoRepository<User, ObjectId> {
    User findByUsername(String username);
    User findByUid(Long uid);
}
