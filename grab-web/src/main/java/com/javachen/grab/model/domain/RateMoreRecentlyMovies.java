package com.javachen.grab.model.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;
import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * @author june
 * @createTime 2019-07-08 13:27
 * @see
 * @since
 */
@Data
@Document(collection = "RateMoreRecentlyMovies")
public class RateMoreRecentlyMovies {
    @Id
    @JsonIgnore
    private ObjectId _id;

    private Long mid;

    private Integer num;

    private String yearmonth;

}
