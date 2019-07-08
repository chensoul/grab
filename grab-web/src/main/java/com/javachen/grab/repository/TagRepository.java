package com.javachen.grab.repository;

import com.javachen.grab.model.domain.Tag;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.repository.MongoRepository;

import java.util.List;

/**
 * @author june
 * @createTime 2019-07-07 22:51
 * @see
 * @since
 */
public interface TagRepository extends MongoRepository<Tag, ObjectId> {
    List<Tag> findAllByMid(Long mid);

    List<Tag>  findAllByUidAndMid(Long uid,Long mid);

}
